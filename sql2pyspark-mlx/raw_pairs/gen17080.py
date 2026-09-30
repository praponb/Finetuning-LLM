from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode("attributes").alias("attr_key", "v"))

result = (
    exploded
    .select(
        "id",
        "created_at",
        "attr_key",
        F.when(F.col("attr_key").rlike("^(device|gift)$"), "GROUP_A").when(F.col("attr_key").rlike("^(utm_source|weight_kg)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key_zone").desc())
)
