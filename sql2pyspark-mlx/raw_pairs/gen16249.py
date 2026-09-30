from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(~F.col("channel").eqNullSafe(F.lit("web")))
    .select(
        "*",
        F.length("promo_code").alias("promo_code_length"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "promo_code_length",
        "channel_filled",
        F.max("promo_code_length").over(w).alias("running_max_promo_code_length"),
        F.sum("promo_code_length").over(w).alias("running_sum_promo_code_length"),
        F.count("promo_code_length").over(w).alias("running_count_promo_code_length"),
        F.avg("promo_code_length").over(w).alias("running_avg_promo_code_length"),
    )
)

cleaned = (
    with_windows
    .filter(~F.col("channel").eqNullSafe(F.lit("store")))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "promo_code_length",
        "channel_filled",
        "running_sum_promo_code_length",
        F.coalesce("running_max_promo_code_length", F.lit(0)).alias("running_max_promo_code_length_filled"),
    )
)

matched = (
    cleaned
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .withColumn("running_sum_promo_code_length_tier", F.when(F.col("running_sum_promo_code_length") >= 370, "HIGH").otherwise("LOW"))
    .rollup("running_sum_promo_code_length_tier")
    .agg(
        F.count_if(F.col("promotion_promo_code") == "WELCOME5").alias("welcome5_events"),
        F.sum(F.when(F.col("promotion_promo_code") == "SPRING10", F.col("promo_code_length")).otherwise(0)).alias("spring10_promo_code_length"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.percentile_approx("promo_code_length", [0.25, 0.5, 0.75]).alias("promo_code_length_quartiles"),
        F.grouping_id().alias("gid"),
    )
)
