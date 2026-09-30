from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

lines = flattened.select("key", F.explode("coupon_codes").alias("value"))

filtered = (
    lines
    .filter(F.col("key").isin(["gift", "utm_source", "weight_kg"]))
    .withColumn("clean_key", F.trim("key"))
)

base = (
    filtered
    .filter(F.col("key").isNotNull() & F.col("value").isin(["SPRING10", "WELCOME5"]))
    .select(
        "*",
        F.concat_ws("-", "key", "value").alias("key_value_key"),
        F.coalesce("value", F.lit("Unknown")).alias("value_filled"),
        F.coalesce("key", F.lit("OTHER")).alias("key_filled"),
    )
)

result = (
    base
    .select(
        "key",
        "clean_key",
        "key_value_key",
        "value_filled",
        "key_filled",
        F.when(F.col("key").rlike("^(device|weight_kg)$"), "PRIORITY").when(F.col("key").rlike("^(utm_source|gift)$"), "GROUP_B").otherwise("GROUP_A").alias("key_zone"),
    )
)
