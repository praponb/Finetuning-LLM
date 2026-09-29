from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("value"))

cleaned = (
    flattened
    .filter(F.col("value").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("value")).alias("value_title"),
        F.coalesce("value", F.lit("Unknown")).alias("value_filled"),
    )
)

matched_keys = (
    cleaned
    .select("id")
    .intersect(
        df_carts
        .filter(F.array_contains("coupon_codes", "SPRING10"))
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 14:00:00")))
    .withColumn("created_at_label", F.date_format("created_at", "yyyy-MM-dd"))
)

daily = (
    base
    .groupBy("value")
    .agg(
        F.countDistinct("value").alias("distinct_values"),
        F.size(F.collect_set("value")).alias("num_values"),
        F.count("*").alias("cnt"),
    )
)

result = (
    daily
    .select("value", "num_values", "cnt")
    .orderBy(F.col("cnt").asc_nulls_first())
)
