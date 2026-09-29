from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-03-06").cast("date")) & (F.col("status").isin("CANCELLED", "EXPIRED", "ACTIVE")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
    )
)

enriched = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.status_ref",
        "s.product_ref",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

joined_holder = (
    enriched.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.status_ref",
        "s.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

result = (
    joined_holder
    .groupingSets(
        [["holder_id", "holder_region"], ["holder_id"], ["holder_region"]],
        "holder_id", "holder_region",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(100 * F.count(F.when(~F.col("holder_region").isin("West", "South"), 1)) / F.count("*"), 1).alias("pct_holder_region_not_in_west_so"),
    )
    .select(
        "holder_id",
        F.coalesce(F.col("holder_region"), F.lit("ALL")).alias("holder_region"),
        "cnt",
        "distinct_ids",
        "pct_holder_region_not_in_west_so",
    )
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("holder_region").asc_nulls_last(),
    )
)
