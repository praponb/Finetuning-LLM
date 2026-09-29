from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("days_since_ends_on", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("promo_code")
w_recent = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc())
w5 = Window.partitionBy("running_count_last3_avg_discount_pct_tier").orderBy(F.col("mobile_running_sum_last3_avg_discount_pct_whole").desc())

filtered = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-07-23").cast("date")) & (F.col("starts_on") <= "2025-07-05"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.length("channel").alias("channel_length"),
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
    )
)

enriched = (
    filtered
    .withColumn("last3_min_discount_pct", F.min("discount_pct").over(w))
    .withColumn("last3_avg_discount_pct", F.avg("discount_pct").over(w))
    .withColumn("last3_count_discount_pct", F.count("discount_pct").over(w))
    .withColumn("promo_code_stddev_samp_discount_pct", F.stddev_samp("discount_pct").over(w2))
)

with_windows = (
    enriched
    .withColumn("running_count_last3_avg_discount_pct", F.count("last3_avg_discount_pct").over(w_recent))
    .withColumn("running_max_last3_avg_discount_pct", F.max("last3_avg_discount_pct").over(w_recent))
    .withColumn("running_sum_last3_avg_discount_pct", F.sum("last3_avg_discount_pct").over(w_recent))
)

deduped = (
    with_windows
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

valid = (
    deduped
    .filter(F.col("starts_on") > F.lit("2023-09-02").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "last3_avg_discount_pct",
        "last3_count_discount_pct",
        "promo_code_stddev_samp_discount_pct",
        "running_count_last3_avg_discount_pct",
        "row_num",
        F.floor("running_sum_last3_avg_discount_pct").alias("running_sum_last3_avg_discount_pct_whole"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("running_count_last3_avg_discount_pct") >= 13, "LARGE").otherwise("SMALL").alias("running_count_last3_avg_discount_pct_tier"),
        F.when(F.col("promo_code_stddev_samp_discount_pct") >= 0, "HIGH").otherwise("LOW").alias("promo_code_stddev_samp_discount_pct_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("running_sum_last3_avg_discount_pct_whole")).otherwise(0)).alias("mobile_running_sum_last3_avg_discount_pct_whole"),
        F.round(F.avg("running_count_last3_avg_discount_pct"), 2).alias("avg_running_count_last3_avg_discount_pct"),
        F.percentile_approx("running_sum_last3_avg_discount_pct_whole", [0.25, 0.5, 0.75]).alias("running_sum_last3_avg_discount_pct_whole_quartiles"),
    )
    .withColumn("mobile_running_sum_last3_avg_discount_pct_whole_rank", F.rank().over(w5))
)
