from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

flattened = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "key",
        "v",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

with_dims = (
    flattened
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(~F.col("c.is_active"))
    .select("s.*", F.col("c.email"))
)

filtered = (
    with_dims
    .filter((F.col("created_at") > F.lit("2025-03-01 17:00:00").cast("timestamp")) & F.col("key").isNotNull())
    .withColumn("key_email_key", F.concat_ws("-", "key", "email"))
)

cleaned = (
    filtered
    .filter((F.col("created_at") > F.lit("2025-03-01 22:00:00").cast("timestamp")) & F.col("key").isin(["weight_kg", "gift", "utm_source"]))
    .select(
        "id",
        "created_at",
        "key",
        "v",
        "email",
        "key_email_key",
        F.lower("key").alias("key_lower"),
    )
)

valid = (
    cleaned
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & (F.col("key").eqNullSafe(F.lit("weight_kg"))))
    .select("*", F.regexp_replace("email", " ", "").alias("email_slug"))
)

cleaned_carts = (
    valid
    .filter(F.length("email") > 6)
    .withColumn("created_at_weekofyear", F.weekofyear("created_at"))
    .withColumn("key_clean", F.coalesce("key", F.lit("UNKNOWN")))
)

result = (
    cleaned_carts
    .groupBy(
        F.when(F.col("created_at_weekofyear") >= 64, "HIGH").otherwise("LOW").alias("created_at_weekofyear_tier"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.count_if(F.col("key") == "device").alias("device_events"),
        F.count(F.when(F.col("key") == "device", 1)).alias("device_count"),
        F.sum(F.when(F.col("key") == "utm_source", F.col("created_at_weekofyear"))).alias("utm_source_total_created_at_weekofyear"),
        F.min("created_at").alias("first_created_at"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
    )
    .orderBy(F.col("device_count").desc_nulls_first())
)
