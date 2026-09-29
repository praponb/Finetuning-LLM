from pyspark.sql import functions as F

filtered = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

combined_details_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.segment").isNotNull())
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
    )
)

combined_details = (
    combined_details_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.holder_joined_on",
        "s.holder_segment",
        F.col("p.segment").alias("holder_segment_2"),
        F.col("p.joined_on").alias("holder_joined_on_2"),
    )
)

result = (
    combined_details
    .rollup("holder_id", "holder_segment_2", "holder_segment")
    .agg(F.round(F.sum("premium"), 2).alias("total_premium"))
    .select(
        "holder_id",
        F.coalesce(F.col("holder_segment_2"), F.lit("ALL")).alias("holder_segment_2"),
        F.coalesce(F.col("holder_segment"), F.lit("ALL")).alias("holder_segment"),
        "total_premium",
    )
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("holder_segment_2").asc_nulls_last(),
        F.col("holder_segment").asc_nulls_last(),
    )
)
