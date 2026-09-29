from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

exploded = (
    flattened
    .select("id", "k", "attr_value", F.explode_outer("coupon_codes").alias("element"))
)

valid = (
    exploded
    .filter(F.col("element").isNotNull() & (F.col("element") != "WELCOME5"))
    .withColumn("k_group", F.when(F.col("k").isin(["weight_kg", "utm_source"]), "Tier 1").otherwise("Other"))
    .withColumn("element_lower", F.lower("element"))
)

result = (
    valid
    .select("id", "k", "attr_value", "element", "k_group", "element_lower")
    .orderBy("k")
)
