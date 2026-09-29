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

exploded_carts = (
    exploded
    .select(
        "id",
        "attr_key",
        "attr_value",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

cleaned = (
    exploded_carts
    .filter(F.col("value").isNull())
    .withColumn("value_group", F.when(F.col("value").isin(["SPRING10", "WELCOME5"]), "Core").otherwise("Other"))
    .withColumn("value_group_2", F.when(F.col("value").isin(["VIP", "WELCOME5"]), "Tier 1").otherwise("Other"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("N/A")))
)

valid = (
    cleaned
    .filter(F.col("value") == "BUNDLE")
    .select(
        "id",
        "attr_key",
        "attr_value",
        "value",
        "value_group",
        "value_group_2",
        "attr_key_filled",
        F.when(F.col("attr_key").isin(["weight_kg", "gift"]), "Group A").otherwise("Other").alias("attr_key_group"),
        F.length("value").alias("value_length"),
        F.coalesce("value", F.lit("OTHER")).alias("value_clean"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("value_length") >= 87, "LARGE").otherwise("SMALL").alias("value_length_tier"),
    )
    .agg(
        F.count_if(F.col("attr_key") == "utm_source").alias("utm_source_events"),
        F.count(F.when(F.col("attr_key") == "device", 1)).alias("device_rows"),
        F.count(F.when(F.col("value") == "WELCOME5", 1)).alias("welcome5_count"),
    )
)
