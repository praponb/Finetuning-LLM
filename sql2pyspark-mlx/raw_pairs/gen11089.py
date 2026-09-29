from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("channel").eqNullSafe(F.lit("partner"))))
    .withColumn("channel_title", F.initcap(F.trim("channel")))
)

matched_keys = (
    base
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("store_id").isin([7, 15]))
        .select("id")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.signup_date") > "2025-09-01")
    .select(
        "src.customer_id",
        "src.store_id",
        "src.order_date",
        "src.status",
        "src.channel",
        F.col("d.signup_date"),
        F.col("d.age"),
    )
)

filtered = (
    with_dims
    .filter(F.col("channel").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "*",
        F.col("age").cast("double").alias("age_str"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

base_orders = (
    filtered
    .filter((F.col("age_str") >= 61) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.date_format("signup_date", "yyyy-MM").alias("signup_date_label"),
        F.last_day("signup_date").alias("signup_date_month_end"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

result = (
    base_orders
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["city", "channel"], ["city"]],
        F.col("s.city").alias("city"),
        F.col("e.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.age_str")).alias("max_age_str"),
    )
)
