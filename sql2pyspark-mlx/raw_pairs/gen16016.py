from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

lines_carts = lines.select("key", "attr_value", F.explode("coupon_codes").alias("element"))

filtered = (
    lines_carts
    .filter(F.col("element").isNotNull() & F.col("element").isNotNull())
    .select(
        "*",
        F.concat("key", F.lit("/"), "element").alias("key_element"),
        F.coalesce("key", F.lit("N/A")).alias("key_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("element").isNotNull() & ~F.col("key").isin(["weight_kg", "gift", "device"]))
    .select(
        "attr_value",
        "element",
        "key_element",
        F.trim("element").alias("clean_element"),
        F.coalesce("element", F.lit("DIRECT")).alias("element_filled"),
    )
)

result = (
    valid
    .select(
        "attr_value",
        "element",
        "key_element",
        "clean_element",
        "element_filled",
        F.when(F.col("element").rlike("^(SPRING10|VIP)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("element_zone"),
    )
)
