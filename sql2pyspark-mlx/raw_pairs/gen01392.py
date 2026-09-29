from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("key", "attr_value"))
)

result = (
    lines
    .select(
        "id",
        "created_at",
        "key",
        F.when(F.col("key").rlike("^(device|gift)$"), "CORE").when(F.col("key").rlike("^(utm_source|weight_kg)$"), "GROUP_B").otherwise("GROUP_A").alias("key_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
