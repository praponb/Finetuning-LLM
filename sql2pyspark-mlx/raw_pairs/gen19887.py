from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

flattened_carts = flattened.select("id", "key", F.explode("coupon_codes").alias("code"))

cleaned = (
    flattened_carts
    .filter(F.col("key").isNotNull() & (F.col("code") != "WELCOME5"))
    .withColumn("code_upper", F.upper("code"))
    .withColumn("code_filled", F.coalesce("code", F.lit("Unknown")))
)

filtered = (
    cleaned
    .filter(F.col("key").isin(["utm_source", "device"]))
    .select(
        "key",
        "code",
        "code_upper",
        F.initcap(F.trim("key")).alias("key_title"),
        F.length("key").alias("key_length"),
        F.coalesce("key", F.lit("NONE")).alias("resolved_key"),
    )
)

cleaned_carts = (
    filtered
    .filter(F.col("code").isNotNull())
    .withColumn("code_filled", F.coalesce("code", F.lit("N/A")))
)

summary = (
    cleaned_carts
    .groupBy(
        F.when(F.col("key_length") >= 74, "LARGE").otherwise("SMALL").alias("key_length_tier"),
        "code",
        "key",
    )
    .agg(
        F.max("key_length").alias("max_key_length"),
        F.percentile_approx("key_length", [0.25, 0.5, 0.75]).alias("key_length_quartiles"),
    )
)

result = (
    summary
    .select("key_length_tier", "code", "key", "max_key_length", "key_length_quartiles")
    .orderBy(F.col("key_length_tier").desc_nulls_first(), F.col("key").desc())
)
