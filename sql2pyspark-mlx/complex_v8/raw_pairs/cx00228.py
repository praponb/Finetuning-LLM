from pyspark.sql import functions as F

base = (
    df_policies
    .filter(F.col("holder_id") >= 18)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
)

enriched_holder = (
    base.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

combined_details_holder = (
    enriched_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.holder_risk_score",
        "s.holder_joined_on",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on_2"),
    )
)

result = (
    combined_details_holder
    .cube("holder_region", "product", "status")
    .agg(
        F.round(F.sum("holder_risk_score"), 2).alias("total_holder_risk_score"),
    )
    .select(
        F.coalesce(F.col("holder_region"), F.lit("ALL")).alias("holder_region"),
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "total_holder_risk_score",
    )
    .orderBy(
        F.col("holder_region").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
