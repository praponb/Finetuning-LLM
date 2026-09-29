from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-09-27").cast("date"), F.lit("2026-12-05").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(F.col("product") == "HOME", 1)).alias("policy_product_count"),
    )
)

summarized_rows = (
    scoped_policys.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
    )
)

result = (
    summarized_rows
    .cube("policy_product_count", "status")
    .agg(F.countDistinct("status").alias("distinct_status"))
    .select(
        "policy_product_count",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "distinct_status",
    )
    .orderBy(
        F.col("policy_product_count").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
