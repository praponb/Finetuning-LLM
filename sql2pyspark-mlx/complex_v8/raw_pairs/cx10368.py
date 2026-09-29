from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_or_unknown_holder_id = Window.partitionBy("product_or_unknown", "holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_product_or_unknown_holder_id_2 = Window.partitionBy("product_or_unknown", "holder_id").orderBy("premium", "id")
w_status_product_or_unknown = Window.partitionBy("status", "product_or_unknown")
w_all_by_start_date_full = Window.orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_start_date_month = Window.partitionBy("holder_id", F.date_trunc("month", "start_date"))
w_all_by_start_date = Window.orderBy(F.col("start_date").desc_nulls_first(), "id")
w_holder_id_by_end_date_moving = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)
w_product_or_unknown_holder_id_3 = Window.partitionBy("product_or_unknown", "holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_or_unknown_holder_id_4 = Window.partitionBy("product_or_unknown", "holder_id").orderBy("premium", "id").rowsBetween(-6, -1)
w_start_date_seq_product_or_un = Window.partitionBy("start_date_seq", "product_or_unknown")
w_next3_start_date_year_by_sta = Window.partitionBy(F.year("next3_start_date")).orderBy(F.col("start_date_seq").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_year_product_or_u = Window.partitionBy(F.year("start_date"), "product_or_unknown").orderBy("premium_change", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn_by_start_date_tercile_run = Window.partitionBy("rn").orderBy(F.col("start_date_tercile").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next3_start_date_year_by_sta_2 = Window.partitionBy(F.year("next3_start_date")).orderBy(F.col("start_date_seq").desc_nulls_first(), "id")

valid_policys = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2026-02-26").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

joined_holder = (
    valid_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.product_or_unknown",
        "p.holder_name",
    )
)

metrics = (
    joined_holder
    .select(
        "id",
        "holder_id",
        "start_date",
        "product_or_unknown",
        "holder_name",
        F.min("premium").over(w_product_or_unknown_holder_id).alias("min_premium_so_far"),
        (F.col("premium") - F.lag("premium").over(w_holder_id_by_end_date)).alias("premium_change"),
        F.row_number().over(w_product_or_unknown_holder_id_2).alias("rn"),
        F.round(F.avg("premium").over(w_status_product_or_unknown), 2).alias("status_product_or_unknown_avg_premium"),
        F.nth_value("premium", 2).over(w_all_by_start_date_full).alias("second_premium"),
        F.round(F.avg("premium").over(w_holder_id_start_date_month), 2).alias("holder_id_start_date_month_avg_premium"),
        F.ntile(3).over(w_all_by_start_date).alias("start_date_tercile"),
        F.round(F.avg("premium").over(w_holder_id_by_end_date_moving), 2).alias("premium_avg_3"),
        F.row_number().over(w_all_by_start_date).alias("start_date_seq"),
        F.nth_value("start_date", 2).over(w_product_or_unknown_holder_id_3).alias("second_start_date"),
        F.max("premium").over(w_holder_id_start_date_month).alias("holder_id_start_date_month_max_premium"),
        F.round(F.avg("premium").over(w_product_or_unknown_holder_id_4), 2).alias("premium_avg_6_prior"),
        F.size(F.collect_set("product_or_unknown").over(w_holder_id_start_date_month)).alias("distinct_product_or_unknown_in_holder_id_start_d"),
        F.lead("start_date", 3).over(w_product_or_unknown_holder_id_2).alias("next3_start_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_start_date_month), F.lit(0)), 4).alias("premium_share_of_holder_id_start_date_month"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "start_date",
        "product_or_unknown",
        "status_product_or_unknown_avg_premium",
        "start_date_tercile",
        "premium_avg_3",
        "holder_id_start_date_month_max_premium",
        "distinct_product_or_unknown_in_holder_id_start_d",
        "next3_start_date",
        "premium_share_of_holder_id_start_date_month",
        F.round(F.col("min_premium_so_far") / F.nullif(F.sum("min_premium_so_far").over(w_start_date_seq_product_or_un), F.lit(0)), 4).alias("min_premium_so_far_share_of_start_date_seq_produ"),
        F.count(F.lit(1)).over(w_next3_start_date_year_by_sta).alias("running_count"),
        F.first("second_premium", ignorenulls=True).over(w_start_date_year_product_or_u).alias("first_known_second_premium"),
        F.sum("rn").over(w_rn_by_start_date_tercile_run).alias("running_rn"),
        F.ntile(10).over(w_next3_start_date_year_by_sta_2).alias("start_date_seq_decile"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "start_date",
        "product_or_unknown",
        "status_product_or_unknown_avg_premium",
        "start_date_tercile",
        "next3_start_date",
        "premium_share_of_holder_id_start_date_month",
        "running_count",
        "running_rn",
        F.round(F.col("premium_avg_3") * 1.5, 2).alias("premium_avg_3_adj"),
        F.when(F.col("premium_avg_3") >= 3320.0, "HIGH").when(F.col("premium_avg_3") >= 1880.0, "MEDIUM").otherwise("LOW").alias("premium_avg_3_tier"),
    )
    .orderBy("premium_avg_3_tier")
)
