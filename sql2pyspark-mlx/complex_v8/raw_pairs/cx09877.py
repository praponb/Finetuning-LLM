from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter((F.col("holder_id") <= 2) & (~F.col("status").isin("LAPSED", "ACTIVE")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.abs(F.col("premium") - 2721.8).alias("premium_distance"),
    )
)

derived = (
    prepared_policys
    .select(
        "*",
        F.abs(F.col("premium_distance") - 2750.0).alias("premium_distance_distance"),
        F.coalesce(F.col("premium_distance"), F.lit(0.0)).alias("premium_distance_filled"),
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
    )
)

combined_details = (
    derived.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.premium_distance",
        "s.premium_distance_distance",
        "s.product_ref",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    combined_details
    .select("holder_id")
    .distinct()
    .orderBy("holder_id")
)
