from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode("attributes").alias("key", "v"))

result = (
    lines
    .select(
        "id",
        "created_at",
        "key",
        "v",
        F.when(F.col("key").rlike("^(utm_source|gift)$"), "GROUP_A").when(F.col("key").rlike("^(device|weight_kg)$"), "OTHER_ZONE").otherwise("CORE").alias("key_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first(), F.col("created_at").desc())
)
