from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_prior_3 = Window.orderBy("start_date", "id").rowsBetween(-3, -1)
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_start_date_month_holder_id = Window.partitionBy(F.month("start_date"), "holder_id")

valid_policys = (
    df_policies
    .filter(F.col("status") == "EXPIRED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_windows = (
    valid_policys
    .select(
        "*",
        F.round(F.avg("premium").over(w_all_by_start_date_prior_3), 2).alias("premium_avg_3_prior"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_all_by_start_date)).alias("days_since_prev_start_date"),
        F.round(F.avg("premium").over(w_start_date_month_holder_id), 2).alias("start_date_month_holder_id_avg_premium"),
    )
)

with_parent = (
    with_windows.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.holder_name").isNotNull())
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_avg_3_prior",
        "s.days_since_prev_start_date",
        "s.start_date_month_holder_id_avg_premium",
        "p.holder_name",
    )
)

result = (
    with_parent
    .groupingSets(
        [["status", "holder_id"], ["status"]],
        "status", "holder_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.sum("days_since_prev_start_date").alias("total_days_since_prev_start_date"),
    )
)
