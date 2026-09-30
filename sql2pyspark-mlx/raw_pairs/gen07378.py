from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

flattened = lines.select("created_at", "promo", F.explode("coupon_codes").alias("value"))

valid = (
    flattened
    .filter(F.col("value").isin(["BUNDLE", "WELCOME5"]))
    .withColumn("promo_group", F.when(F.col("promo").isin(["BUNDLE", "VIP"]), "Tier 1").otherwise("Other"))
)

valid_carts = (
    valid
    .filter(F.col("promo").isNotNull() & F.col("value").isNotNull())
    .select(
        "*",
        F.trim("promo").alias("clean_promo"),
        F.coalesce("value", F.lit("OTHER")).alias("value_filled"),
    )
)

base = (
    valid_carts
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.col("value").isNotNull())
    .withColumn("value_upper", F.upper("value"))
    .withColumn("clean_promo_2", F.trim("promo"))
    .withColumn("resolved_value", F.coalesce("value", F.lit("DIRECT")))
)

cleaned = (
    base
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-01"))) & F.col("value").isNotNull())
    .select(
        "created_at",
        "promo",
        "value",
        "value_filled",
        "clean_promo_2",
        F.substring("promo", 1, 4).alias("promo_prefix"),
        F.coalesce("value", F.lit("UNKNOWN")).alias("value_filled_2"),
    )
)

filtered = (
    cleaned
    .filter(F.col("promo").isNotNull())
    .select(
        "created_at",
        "promo",
        "value",
        "value_filled",
        "clean_promo_2",
        "promo_prefix",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
    )
)

result = (
    filtered
    .select(
        "created_at",
        "value_filled",
        "clean_promo_2",
        "promo_prefix",
        "days_since_created_at",
    )
    .orderBy("created_at")
)
