from pyspark.sql import functions as F

exploded = df_carts.select("id", F.explode_outer("coupon_codes").alias("element"))

filtered = (
    exploded
    .filter(F.col("element").isNotNull())
    .select("*", F.length("element").alias("element_length"))
)

cleaned = (
    filtered
    .filter(F.col("element") != "WELCOME5")
    .withColumn("element_filled", F.coalesce("element", F.lit("Unknown")))
    .withColumn("resolved_element", F.coalesce("element", F.lit("N/A")))
)

base = (
    cleaned
    .filter((~F.col("element").eqNullSafe(F.lit("WELCOME5"))) & (F.col("element_length") < 43))
    .select(
        "*",
        F.upper("element").alias("element_upper"),
        F.coalesce("element", F.lit("NONE")).alias("element_clean"),
    )
)

result = (
    base
    .select(
        "id",
        "element",
        "element_length",
        "element_filled",
        "resolved_element",
        "element_upper",
    )
)
