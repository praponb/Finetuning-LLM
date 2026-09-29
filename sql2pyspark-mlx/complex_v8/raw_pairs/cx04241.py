from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_product_count_status = Window.partitionBy("policy_product_count", "status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_product_count_status_2 = Window.partitionBy("policy_product_count", "status").orderBy("start_date")
w_start_date_month_product = Window.partitionBy(F.date_trunc("month", "start_date"), "product")

scoped = (
    df_policies
    .filter((F.col("premium") < 3465.4) & (F.col("status") == "ACTIVE"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("start_date").alias("last_policy_start_date"),
        F.count(F.when(F.col("product") == "AUTO", 1)).alias("policy_product_count"),
        F.max("premium").alias("max_policy_premium"),
    )
)

with_stats = (
    scoped.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.last_policy_start_date",
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
        "p.max_policy_premium",
    )
)

holder_id_stats = (
    with_stats
    .groupBy("holder_id")
    .agg(F.max("max_policy_premium").alias("max_max_policy_premium"))
)

outliers = (
    with_stats.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.max_policy_premium") == F.col("g.max_max_policy_premium"))
    .select(
        "s.*",
        "g.max_max_policy_premium",
        F.round(F.col("s.max_policy_premium") - F.col("g.max_max_policy_premium"), 2).alias("max_policy_premium_minus_max"),
    )
)

filtered = (
    outliers.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.region") == "South"), "left_anti")
)

metrics = (
    filtered
    .select(
        "*",
        F.first("end_date", ignorenulls=True).over(w_policy_product_count_status).alias("first_known_end_date"),
        F.round(F.cume_dist().over(w_policy_product_count_status_2), 3).alias("start_date_cume_dist"),
        F.round(F.avg("max_max_policy_premium").over(w_start_date_month_product), 2).alias("start_date_month_product_avg_max_max_policy_prem"),
        F.rank().over(w_policy_product_count_status_2).alias("start_date_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        "last_policy_start_date",
        "policy_product_count",
        "max_policy_premium",
        "max_max_policy_premium",
        "max_policy_premium_minus_max",
        "first_known_end_date",
        "start_date_month_product_avg_max_max_policy_prem",
        "start_date_rank",
    )
    .orderBy(F.col("max_policy_premium").desc(), "status", "id")
    .limit(20)
)
