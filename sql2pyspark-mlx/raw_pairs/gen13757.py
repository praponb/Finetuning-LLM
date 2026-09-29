from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "coupon_codes", F.explode_outer("attributes").alias("key", "value"))
)

flattened = lines.select("id", "value", F.explode("coupon_codes").alias("value_2"))

cleaned = (
    flattened
    .filter(F.col("value_2").isin(["VIP", "SPRING10", "WELCOME5"]))
    .withColumn("value_2_filled", F.coalesce("value_2", F.lit("Unknown")))
    .withColumn("value_2_filled_2", F.coalesce("value_2", F.lit("OTHER")))
)

filtered = (
    cleaned
    .filter(F.col("value_2") == "WELCOME5")
    .select(
        "*",
        F.initcap(F.trim("value_2")).alias("value_2_title"),
        F.trim("value_2").alias("clean_value_2"),
        F.coalesce("value_2", F.lit("N/A")).alias("value_2_clean"),
    )
)

filtered_carts = (
    filtered
    .filter(F.col("value_2").isNotNull())
    .select("*", F.substring("value_2", 1, 3).alias("value_2_prefix"))
)

valid = (
    filtered_carts
    .filter(F.col("value_2").isin(["SPRING10", "WELCOME5", "BUNDLE"]))
    .select(
        "id",
        "value_2",
        "value_2_filled",
        "clean_value_2",
        "value_2_clean",
        "value_2_prefix",
        F.lower("value_2").alias("value_2_lower"),
        F.when(F.col("value_2") == "VIP", 1).otherwise(0).alias("is_vip"),
        F.coalesce("value_2", F.lit("NONE")).alias("value_2_clean_2"),
    )
)

valid_carts = (
    valid
    .filter(F.col("value_2").isNotNull())
    .select(
        "*",
        F.round((F.col("is_vip") * 0.85), 2).alias("adjusted_is_vip"),
        F.when(F.col("value_2") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
    )
)

result = (
    valid_carts
    .rollup(
        F.when(F.col("adjusted_is_vip") >= 64, "LARGE").otherwise("SMALL").alias("adjusted_is_vip_tier"),
        "value_2",
    )
    .agg(
        F.count(F.when(F.col("value_2") == "VIP", 1)).alias("vip_rows"),
        F.round(F.avg("is_vip"), 2).alias("avg_is_vip"),
        F.count("*").alias("event_count"),
        F.round(F.sum("is_vip") / F.nullif(F.count(F.when(F.col("value_2") == "BUNDLE", 1)), F.lit(0)), 2).alias("is_vip_per_bundle"),
        F.percentile_approx("is_vip", [0.25, 0.5, 0.75]).alias("is_vip_quartiles"),
        F.sort_array(F.collect_set("value_2")).alias("value_2_list"),
    )
)
