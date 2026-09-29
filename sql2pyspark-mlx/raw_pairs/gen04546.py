from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

result = (
    exploded
    .select(
        "id",
        "coupon_codes",
        "key",
        "v",
        F.when(F.col("key").rlike("^(weight_kg|device)$"), "GROUP_B").otherwise("GROUP_A").alias("key_zone"),
    )
    .orderBy(F.col("key_zone").desc_nulls_first())
)
