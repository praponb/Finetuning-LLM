from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

exploded_carts = exploded.select("id", "value", F.explode_outer("coupon_codes").alias("element"))

valid = (
    exploded_carts
    .filter(F.col("element").isNotNull())
    .withColumn("element_lower", F.lower("element"))
    .withColumn("element_clean", F.coalesce("element", F.lit("NONE")))
)

filtered = (
    valid
    .filter(F.col("element").isin(["SPRING10", "BUNDLE", "VIP"]))
    .select(
        "id",
        "value",
        "element",
        "element_lower",
        "element_clean",
        F.trim("element").alias("clean_element"),
    )
)

result = (
    filtered
    .select("id", "element", "element_lower", "element_clean")
    .orderBy(F.col("element").asc_nulls_first())
)
