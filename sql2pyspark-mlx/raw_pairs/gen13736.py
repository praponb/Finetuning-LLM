from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("value"))

valid = (
    lines
    .filter(F.col("value").isin(["BUNDLE", "SPRING10", "VIP"]))
    .withColumn("value_lower", F.lower("value"))
    .withColumn("value_prefix", F.substring("value", 1, 2))
    .withColumn("value_filled", F.coalesce("value", F.lit("DIRECT")))
)

kept_keys = (
    valid
    .select("customer_id", "id")
    .exceptAll(
        df_carts
        .filter(F.col("customer_id").isin([2, 35]))
        .select("customer_id", "id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("src.*", F.col("b.name"))
)

valid_carts = (
    joined
    .filter(F.col("value") == "BUNDLE")
    .select(
        "id",
        "customer_id",
        "value",
        "value_lower",
        "value_prefix",
        "value_filled",
        F.coalesce("name", F.lit("Unknown")).alias("name_filled"),
        F.coalesce("value", F.lit("UNKNOWN")).alias("resolved_value"),
    )
)

cleaned = (
    valid_carts
    .filter(F.col("value").eqNullSafe(F.lit("BUNDLE")))
    .select(
        "id",
        "customer_id",
        "value",
        "value_lower",
        "value_prefix",
        "value_filled",
        "name_filled",
        "resolved_value",
        F.coalesce("value", F.lit("Unknown")).alias("value_filled_2"),
        F.when(F.col("value") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
        F.coalesce("value", F.lit("NONE")).alias("value_clean"),
    )
)

joined_carts = (
    cleaned
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select("e.*", F.col("c.segment"), F.col("c.country"))
)

result = (
    joined_carts
    .groupBy("value")
    .pivot("segment", ["Corporate", "Consumer"])
    .agg(F.avg("is_welcome5"))
)
