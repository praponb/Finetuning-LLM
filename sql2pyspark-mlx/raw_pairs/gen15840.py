from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

lines = (
    flattened
    .select("id", "created_at", "key", F.explode_outer("coupon_codes").alias("element"))
)

base = (
    lines
    .filter(F.col("created_at") >= F.lit("2025-03-01 21:00:00").cast("timestamp"))
    .select("*", F.concat("key", F.lit(", "), "element").alias("key_element"))
)

cleaned = (
    base
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-01"))) & F.col("element").isNotNull())
    .select(
        "id",
        "key",
        (F.datediff(F.current_date(), "created_at") / 7).alias("weeks_since_created_at"),
    )
)

result = cleaned.select("key")
