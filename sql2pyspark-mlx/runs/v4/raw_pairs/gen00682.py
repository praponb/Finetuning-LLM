from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode("attributes").alias("key", "v"))

result = (
    exploded
    .select(
        "id",
        "key",
        F.when(F.col("key").rlike("^(utm_source|gift)$"), "EXTENDED").when(F.col("key").rlike("^(device|weight_kg)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("key_zone"),
    )
    .orderBy(F.col("key").desc_nulls_last())
)
