from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter(F.col("product") == "AUTO")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("policy_status_count"),
    )
)

with_stats = (
    prepared.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
    )
)

joined = (
    with_stats.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.region").isin("Central", "South", "West"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.product_len",
        F.col("p.region").alias("holder_region"),
    )
)

product_stats = (
    joined
    .groupBy("product")
    .agg(F.avg("premium").alias("avg_premium"))
)

outliers = (
    joined.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

result = (
    outliers
    .filter(F.col("start_date") < F.lit("2025-03-23").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        "product_len",
        "holder_region",
        "avg_premium",
    )
    .orderBy("product_len", "product")
)
