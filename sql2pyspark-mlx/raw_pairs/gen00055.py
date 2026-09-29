from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .groupBy("k")
    .agg(
        F.count(F.when(F.col("k") == "device", 1)).alias("device_count"),
        F.count(F.when(F.col("k") == "gift", 1)).alias("gift_count"),
    )
    .orderBy(F.col("k").desc_nulls_first())
)
