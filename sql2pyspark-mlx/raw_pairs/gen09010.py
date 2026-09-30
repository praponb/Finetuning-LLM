from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy(F.date_trunc("MONTH", "starts_on"), "promo_code").orderBy(F.col("channel_length").desc(), "id")
w3 = Window.partitionBy("channel", "promo_code").orderBy(F.col("channel_length").desc(), "id")
w_ordered = Window.orderBy("channel_length_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("channel") != "store"))
    .withColumn("channel_length", F.length("channel"))
    .withColumn("promo_code_channel_key", F.concat_ws(",", "promo_code", "channel"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

with_windows = (
    filtered
    .select(
        "*",
        F.avg("channel_length").over(w).alias("channel_length_avg_3_prior"),
        F.ntile(3).over(w2).alias("channel_length_bucket"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "channel_length",
        "resolved_channel",
        "channel_length_avg_3_prior",
        "channel_length_bucket",
    )
)

valid = (
    latest
    .filter(F.col("ends_on") > F.lit("2023-10-12").cast("date"))
    .select(
        "*",
        (F.col("channel_length_bucket") * F.col("discount_pct")).alias("channel_length_bucket_times_discount_pct"),
    )
)

with_windows_promotions = (
    valid
    .withColumn("running_count_channel_length_avg_3_prior", F.count("channel_length_avg_3_prior").over(w_ordered))
    .withColumn("running_max_channel_length_avg_3_prior", F.max("channel_length_avg_3_prior").over(w_ordered))
    .withColumn("running_avg_channel_length_avg_3_prior", F.avg("channel_length_avg_3_prior").over(w_ordered))
)

result = (
    with_windows_promotions
    .groupBy(
        F.when(F.col("channel_length_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("channel_length_bucket_tier"),
        F.date_trunc("year", "ends_on").alias("year_start"),
        F.when(F.col("running_avg_channel_length_avg_3_prior") >= 88, "HIGH").otherwise("LOW").alias("running_avg_channel_length_avg_3_prior_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("channel_length_bucket", 0.95).alias("channel_length_bucket_p95"),
    )
    .orderBy(F.col("channel_length_bucket_p95").asc_nulls_last())
)
