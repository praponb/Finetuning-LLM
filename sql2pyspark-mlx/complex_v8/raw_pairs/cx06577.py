from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_holder_risk_score = Window.partitionBy("product").orderBy(F.col("holder_risk_score").desc(), "id")
w_start_date_month_by_start_da = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id = Window.partitionBy("holder_id")
w_product_start_date_month = Window.partitionBy("product", F.date_trunc("month", "start_date"))
w_status = Window.partitionBy("status")
w_all_by_start_date_full = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_running = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_product_by_premium_fu = Window.partitionBy("status", "product").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_moving_1 = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(-1, Window.currentRow)

prepared_policys = (
    df_policies
    .filter((F.year(F.col("end_date")) == 2026) | (F.year(F.col("start_date")) == 2024))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("start_date"), "yyyy-MM-dd").alias("start_date_label"),
    )
)

combined_details = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.start_date_label",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_windows = (
    combined_details
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "holder_risk_score",
        F.row_number().over(w_product_by_holder_risk_score).alias("rn"),
        F.sum("premium").over(w_start_date_month_by_start_da).alias("running_premium"),
        F.sum("holder_risk_score").over(w_holder_id).alias("holder_id_sum_holder_risk_score"),
        F.max("premium").over(w_product_start_date_month).alias("product_start_date_month_max_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.ntile(4).over(w_product_by_holder_risk_score).alias("holder_risk_score_quartile"),
        F.nth_value("start_date", 2).over(w_all_by_start_date_full).alias("second_start_date"),
        F.first("start_date", ignorenulls=True).over(w_all_by_start_date_running).alias("first_known_start_date"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_holder_id), F.lit(0)), 4).alias("holder_risk_score_share_of_holder_id"),
        F.min("premium").over(w_product_start_date_month).alias("product_start_date_month_min_premium"),
        F.nth_value("premium", 2).over(w_status_product_by_premium_fu).alias("second_premium"),
        F.max("premium").over(w_all_by_start_date_moving_1).alias("premium_max_2"),
    )
)

result = (
    with_windows
    .filter(F.col("second_start_date").isNotNull())
    .select(
        "id",
        "start_date",
        "holder_risk_score",
        "rn",
        "running_premium",
        "holder_id_sum_holder_risk_score",
        "product_start_date_month_max_premium",
        "holder_risk_score_quartile",
        "second_start_date",
        "first_known_start_date",
        "holder_risk_score_share_of_holder_id",
        "product_start_date_month_min_premium",
        "second_premium",
        "premium_max_2",
    )
    .orderBy(F.col("premium_max_2").desc())
)
