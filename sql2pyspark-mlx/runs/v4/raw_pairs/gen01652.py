from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

lines = (
    exploded
    .select("id", "created_at", "attr_key", F.explode("coupon_codes").alias("element"))
)

cleaned = (
    lines
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-02"))) & F.col("attr_key").isNotNull() & F.col("element").isin(["WELCOME5", "BUNDLE"]))
    .select(
        "*",
        F.initcap(F.trim("attr_key")).alias("attr_key_title"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("attr_key_clean"),
    )
)

base = (
    cleaned
    .filter((F.col("created_at") > F.lit("2025-03-02 12:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .select(
        "*",
        F.lower("attr_key").alias("attr_key_lower"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_clean_2"),
    )
)

filtered = (
    base
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-02"))) & F.col("attr_key").isNotNull())
    .withColumn("created_at_hour", F.hour("created_at"))
    .withColumn("is_device", F.when(F.col("attr_key") == "device", 1).otherwise(0))
)

result = (
    filtered
    .select("created_at", "attr_key", "element", "attr_key_clean", "attr_key_lower")
)
