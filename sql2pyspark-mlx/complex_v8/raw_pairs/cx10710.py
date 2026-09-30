from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter((F.col("start_date").isNotNull()) & (~F.col("status").isin("CANCELLED", "EXPIRED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.when(F.col("status").isin("LAPSED", "ACTIVE"), "group_a").when(F.col("status") == "CANCELLED", "group_b").otherwise("other").alias("status_group"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

joined = (
    base_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status_group",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    joined
    .rollup("product", "status_group", "holder_id")
    .agg(
        F.sum(F.when(F.col("product").isin("HEALTH", "LIFE"), 1).otherwise(0)).alias("n_product_in_health_life"),
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("status_group").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
