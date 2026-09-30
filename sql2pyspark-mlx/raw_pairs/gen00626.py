from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("value"))
)

filtered = (
    flattened
    .filter((F.col("created_at") >= F.lit("2025-03-02 00:00:00").cast("timestamp")) & F.col("value").isNotNull())
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
    .withColumn("created_at_date", F.to_date("created_at"))
    .withColumn("value_clean", F.coalesce("value", F.lit("UNKNOWN")))
)

cleaned = (
    filtered
    .filter(F.col("value").isNull())
    .select(
        "*",
        F.date_format("created_at", "MMM yyyy").alias("created_at_label"),
        F.col("days_since_created_at").cast("double").alias("days_since_created_at_str"),
    )
)

matched_keys = (
    cleaned
    .select("customer_id", "id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isNull())
        .select("customer_id", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (~F.col("value").eqNullSafe(F.lit("WELCOME5"))))
    .select(
        "*",
        F.round("days_since_created_at_str", 2).alias("days_since_created_at_str_rounded"),
    )
)

result = (
    valid
    .select(
        "id",
        "value",
        "days_since_created_at",
        "created_at_date",
        "value_clean",
        "days_since_created_at_str",
        "days_since_created_at_str_rounded",
    )
    .orderBy(F.col("days_since_created_at").desc())
)
