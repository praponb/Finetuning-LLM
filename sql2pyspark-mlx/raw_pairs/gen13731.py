from pyspark.sql import functions as F

flattened = df_carts.select("id", "coupon_codes", F.explode("attributes").alias("key", "value"))

result = (
    flattened
    .select(
        "id",
        "coupon_codes",
        "key",
        F.when(F.col("key").rlike("^(utm_source|weight_kg)$"), "EXTENDED").when(F.col("key").rlike("^(device|gift)$"), "GROUP_B").otherwise("CORE").alias("key_zone"),
    )
    .orderBy(F.col("key").asc_nulls_last())
)
