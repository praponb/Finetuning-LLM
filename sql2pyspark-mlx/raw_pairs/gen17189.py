from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on")
w2 = Window.partitionBy("channel").orderBy(F.col("promo_code_length").desc_nulls_first())
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("starts_on"), "promo_code").orderBy(F.col("promo_code_length").asc_nulls_first())
w6 = Window.partitionBy(F.date_trunc("month", "starts_on"), "channel").orderBy("promo_code_length", "id")
w7 = Window.orderBy(F.col("promo_code_length_cume_dist").desc_nulls_first())
w8 = Window.partitionBy(F.month("starts_on"), "channel").orderBy("running_promo_code_length", "id").rowsBetween(-2, -1)
w9 = Window.partitionBy("channel").orderBy("promo_code_length_cume_dist_avg_2_prior", "id")

base = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-08-02").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.length("promo_code").alias("promo_code_length"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "channel_filled",
        "latest_rank",
        F.cume_dist().over(w2).alias("promo_code_length_cume_dist"),
        F.sum("promo_code_length").over(w3).alias("cumulative_promo_code_length"),
        F.sum("promo_code_length").over(w4).alias("running_promo_code_length"),
        F.dense_rank().over(w5).alias("promo_code_length_dense_rank"),
        F.lag("promo_code_length", 1).over(w6).alias("prev_promo_code_length"),
    )
)

with_windows_promotions = (
    with_windows
    .filter(F.col("cumulative_promo_code_length") == 351)
    .select(
        "id",
        "channel",
        "channel_filled",
        "promo_code_length_cume_dist",
        "cumulative_promo_code_length",
        "prev_promo_code_length",
        F.percent_rank().over(w7).alias("promo_code_length_cume_dist_percent_rank"),
        F.avg("promo_code_length_cume_dist").over(w8).alias("promo_code_length_cume_dist_avg_2_prior"),
    )
)

enriched = (
    with_windows_promotions
    .select(
        "id",
        "channel_filled",
        "promo_code_length_cume_dist",
        "cumulative_promo_code_length",
        "promo_code_length_cume_dist_avg_2_prior",
        F.lead("promo_code_length_cume_dist_percent_rank").over(w9).alias("next_promo_code_length_cume_dist_percent_rank"),
        F.lead("promo_code_length_cume_dist_avg_2_prior").over(w9).alias("next_promo_code_length_cume_dist_avg_2_prior"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("promo_code_length_cume_dist") >= 0.81, "LARGE").otherwise("SMALL").alias("promo_code_length_cume_dist_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("next_promo_code_length_cume_dist_avg_2_prior"), 2).alias("avg_next_promo_code_length_cume_dist_avg_2_prior"),
        F.percentile_approx("next_promo_code_length_cume_dist_avg_2_prior", [0.25, 0.5, 0.75]).alias("next_promo_code_length_cume_dist_avg_2_prior_quartiles"),
        F.round(F.avg("promo_code_length_cume_dist"), 2).alias("avg_promo_code_length_cume_dist"),
        F.max("next_promo_code_length_cume_dist_percent_rank").alias("max_next_promo_code_length_cume_dist_percent_rank"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
