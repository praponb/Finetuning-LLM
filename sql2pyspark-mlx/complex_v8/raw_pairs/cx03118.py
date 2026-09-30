from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.year(F.col("end_date")) == 2025)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.add_months(F.col("end_date"), 1).alias("end_date_plus_1m"),
    )
)

screened = (
    scoped_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.end_date") >= F.lit("2026-01-23").cast("date")), "left_anti")
)

screened_2 = (
    screened.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.start_date") >= F.lit("2025-01-24").cast("date")), "left_anti")
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(F.col("status").isin("CANCELLED", "LAPSED", "ACTIVE"), 1)).alias("policy_status_count"),
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
        F.sum("premium").alias("total_policy_premium"),
    )
)

with_stats = (
    screened_2.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
        "p.avg_policy_premium",
        "p.total_policy_premium",
    )
)

result = (
    with_stats
    .filter(F.col("end_date_plus_1m") >= F.lit("2024-05-01").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "premium_filled",
        "end_date_plus_1m",
        "policy_status_count",
        "avg_policy_premium",
        "total_policy_premium",
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("start_date").desc_nulls_first())
)
