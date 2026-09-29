from pyspark.sql import functions as F

valid_holders = (
    df_policyholders
    .filter(F.col("joined_on").isNotNull())
    .select(
        "id",
        "holder_name",
        "region",
        "segment",
        F.concat(F.col("segment"), F.lit("#"), F.col("id").cast("string")).alias("segment_ref"),
    )
)

holder_rows = (
    valid_holders
    .filter((F.col("segment_ref").isNotNull()) & (F.col("region") != "West"))
    .select(F.col("id").alias("entity_id"), F.col("region").alias("category"))
)

claim_rows = df_claims.select(F.col("id").alias("entity_id"), F.col("claim_type").alias("category"))

result = holder_rows.intersect(claim_rows)
