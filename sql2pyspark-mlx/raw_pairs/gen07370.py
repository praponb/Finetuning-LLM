from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("running_sum_discount_pct").desc(), "id")
w4 = Window.partitionBy("channel").orderBy("running_min_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("running_max_discount_pct_minus_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy(F.year("starts_on")).orderBy("running_min_discount_pct")
w_recent = Window.orderBy("running_min_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_promotions
    .withColumn("running_max_discount_pct", F.max("discount_pct").over(w))
    .withColumn("running_sum_discount_pct", F.sum("discount_pct").over(w))
    .withColumn("running_min_discount_pct", F.min("discount_pct").over(w))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w2))
)

cleaned = (
    metrics
    .filter(F.col("starts_on") > "2023-11-19")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "running_max_discount_pct",
        "running_sum_discount_pct",
        "running_min_discount_pct",
        F.datediff("starts_on", "ends_on").alias("days_ends_on_to_starts_on"),
        (F.col("running_max_discount_pct") - F.col("discount_pct")).alias("running_max_discount_pct_minus_discount_pct"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    ranked
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-09-04"))) & F.col("channel").isNotNull())
    .withColumn("channel_upper", F.upper("channel"))
)

metrics_promotions = (
    filtered
    .select(
        "id",
        "starts_on",
        "discount_pct",
        "running_sum_discount_pct",
        "running_min_discount_pct",
        "days_ends_on_to_starts_on",
        "running_max_discount_pct_minus_discount_pct",
        "latest_rank",
        "channel_upper",
        F.avg("running_max_discount_pct_minus_discount_pct").over(w4).alias("running_avg_running_max_discount_pct_minus_discount_pct"),
        F.count("running_max_discount_pct_minus_discount_pct").over(w4).alias("running_count_running_max_discount_pct_minus_discount_pct"),
        F.max("running_max_discount_pct_minus_discount_pct").over(w4).alias("running_max_running_max_discount_pct_minus_discount_pct"),
        F.last("running_max_discount_pct", ignorenulls=True).over(w5).alias("last_known_running_max_discount_pct"),
    )
)

enriched = (
    metrics_promotions
    .select(
        "id",
        "running_min_discount_pct",
        "latest_rank",
        "channel_upper",
        "running_avg_running_max_discount_pct_minus_discount_pct",
        "running_count_running_max_discount_pct_minus_discount_pct",
        "last_known_running_max_discount_pct",
        F.cume_dist().over(w_part).alias("running_min_discount_pct_cume_dist"),
        F.last("running_count_running_max_discount_pct_minus_discount_pct", ignorenulls=True).over(w_recent).alias("last_known_running_count_running_max_discount_pct_minus_discount_pct"),
    )
)

filtered_promotions = (
    enriched
    .filter(F.col("running_min_discount_pct_cume_dist") < 0.62)
    .select(
        "*",
        (F.col("running_count_running_max_discount_pct_minus_discount_pct") - F.col("latest_rank")).alias("running_count_running_max_discount_pct_minus_discount_pct_minus_latest_rank"),
    )
)

result = (
    filtered_promotions
    .groupBy(
        F.when(F.col("last_known_running_max_discount_pct") >= 0.33, "HIGH").otherwise("LOW").alias("last_known_running_max_discount_pct_tier"),
        F.when(F.col("running_count_running_max_discount_pct_minus_discount_pct_minus_latest_rank") >= 73, "PREMIUM").otherwise("STANDARD").alias("running_count_running_max_discount_pct_minus_discount_pct_minus_latest_rank_tier"),
    )
    .agg(
        F.round(F.avg("running_avg_running_max_discount_pct_minus_discount_pct"), 2).alias("avg_running_avg_running_max_discount_pct_minus_discount_pct"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("last_known_running_max_discount_pct", [0.25, 0.5, 0.75]).alias("last_known_running_max_discount_pct_quartiles"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(
        F.col("avg_running_avg_running_max_discount_pct_minus_discount_pct").desc_nulls_last(),
    )
)
