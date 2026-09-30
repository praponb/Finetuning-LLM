from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag")
w2 = Window.partitionBy("tag").orderBy(F.col("is_utm_source_rounded").asc_nulls_last())
w3 = Window.partitionBy("tag", "attr_key")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "value"),
    )
)

flattened = (
    lines
    .select("id", "attr_key", "value", F.explode_outer("coupon_codes").alias("tag"))
)

cleaned = (
    flattened
    .filter(F.col("tag").eqNullSafe(F.lit("WELCOME5")))
    .withColumn("is_utm_source", F.when(F.col("attr_key") == "utm_source", 1).otherwise(0))
    .withColumn("attr_key_tag", F.concat("attr_key", F.lit(" - "), "tag"))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("N/A")))
)

valid = (
    cleaned
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_group", F.when(F.col("attr_key").isin(["utm_source", "weight_kg"]), "Tier 1").otherwise("Other"))
    .withColumn("is_utm_source_rounded", F.round("is_utm_source", 0))
)

with_windows = (
    valid
    .select(
        "*",
        F.count("is_utm_source_rounded").over(w).alias("tag_count_is_utm_source_rounded"),
        F.percent_rank().over(w2).alias("is_utm_source_rounded_percent_rank"),
        F.max("is_utm_source").over(w3).alias("tag_max_is_utm_source"),
    )
)

result = (
    with_windows
    .withColumn("tag_count_is_utm_source_rounded_tier", F.when(F.col("tag_count_is_utm_source_rounded") >= 18, "LARGE").otherwise("SMALL"))
    .withColumn("is_utm_source_rounded_tier", F.when(F.col("is_utm_source_rounded") >= 72, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["tag_count_is_utm_source_rounded_tier", "is_utm_source_rounded_tier"], ["tag_count_is_utm_source_rounded_tier"]],
        "tag_count_is_utm_source_rounded_tier",
        "is_utm_source_rounded_tier",
    )
    .agg(
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
        F.percentile_approx("is_utm_source_rounded", 0.9).alias("is_utm_source_rounded_p90"),
        F.sum("is_utm_source_rounded").alias("total_is_utm_source_rounded"),
        F.percentile_approx("is_utm_source_rounded", [0.25, 0.5, 0.75]).alias("is_utm_source_rounded_quartiles"),
        F.countDistinct("tag").alias("distinct_tags"),
        F.grouping_id().alias("gid"),
    )
)
