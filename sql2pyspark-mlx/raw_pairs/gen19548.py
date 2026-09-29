from pyspark.sql import functions as F

exploded = df_carts.select("id", "coupon_codes", F.explode("attributes").alias("k", "value"))

flattened = (
    exploded
    .select("id", "k", "value", F.explode_outer("coupon_codes").alias("element"))
)

filtered = (
    flattened
    .filter(~F.col("k").eqNullSafe(F.lit("device")))
    .withColumn("element_title", F.initcap(F.trim("element")))
)

valid = (
    filtered
    .filter(F.col("k") != "gift")
    .select("*", F.lower("k").alias("k_lower"))
)

result = valid.select("id", "k", "value", "element", "element_title", "k_lower")
