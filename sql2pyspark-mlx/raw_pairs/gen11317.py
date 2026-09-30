from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id")
w_running = Window.partitionBy("channel").orderBy("discount_pct")
w4 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w5 = Window.partitionBy("channel").orderBy(F.col("seq_no_2").desc(), "id")
w6 = Window.orderBy("seq_no", "id")
w7 = Window.orderBy(F.col("discount_pct").desc(), "id")
w8 = Window.partitionBy("channel")
w_recent = Window.orderBy("discount_pct_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("prev_seq_no", "id").rowsBetween(-3, -1)
w11 = Window.orderBy("discount_pct_bucket", "id")
w12 = Window.orderBy("running_sum_prev_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy(F.col("prev_seq_no").desc(), "id")

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-11-21").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.concat_ws("-", "channel", "promo_code").alias("channel_promo_code_key"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "channel",
        "discount_pct",
        "channel_promo_code_key",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
        F.percent_rank().over(w_running).alias("discount_pct_percent_rank"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "discount_pct",
        "channel_promo_code_key",
        "seq_no",
        "discount_pct_percent_rank",
        F.lag("seq_no", 1, 0).over(w4).alias("prev_seq_no"),
        F.row_number().over(w5).alias("row_seq"),
        F.ntile(4).over(w6).alias("seq_no_bucket"),
        F.ntile(3).over(w7).alias("discount_pct_bucket"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w8)), 4).alias("seq_no_share"),
    )
)

with_windows = (
    scored
    .filter(F.col("discount_pct_bucket") >= 5)
    .select(
        "id",
        "prev_seq_no",
        "discount_pct_bucket",
        "seq_no_share",
        F.sum("prev_seq_no").over(w_recent).alias("running_sum_prev_seq_no"),
        F.max("prev_seq_no").over(w_recent).alias("running_max_prev_seq_no"),
        F.avg("prev_seq_no").over(w_recent).alias("running_avg_prev_seq_no"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.avg("running_avg_prev_seq_no").over(w10).alias("running_avg_prev_seq_no_avg_3_prior"),
        F.ntile(3).over(w11).alias("discount_pct_bucket_bucket"),
        F.lag("prev_seq_no", 1).over(w11).alias("prev_prev_seq_no"),
    )
)

enriched_promotions = (
    enriched
    .select(
        "id",
        "running_sum_prev_seq_no",
        "running_max_prev_seq_no",
        "running_avg_prev_seq_no",
        "discount_pct_bucket_bucket",
        "prev_prev_seq_no",
        F.first("running_sum_prev_seq_no", ignorenulls=True).over(w12).alias("first_known_running_sum_prev_seq_no"),
        F.row_number().over(w13).alias("event_seq"),
    )
)

result = (
    enriched_promotions
    .groupBy(
        F.when(F.col("discount_pct_bucket_bucket") >= 8, "HIGH").otherwise("LOW").alias("discount_pct_bucket_bucket_tier"),
    )
    .agg(
        F.round(F.avg("discount_pct_bucket_bucket"), 2).alias("avg_discount_pct_bucket_bucket"),
        F.percentile_approx("first_known_running_sum_prev_seq_no", 0.5).alias("first_known_running_sum_prev_seq_no_p50"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
    )
    .filter(F.col("first_known_running_sum_prev_seq_no_p50") >= 0)
    .orderBy(F.col("avg_discount_pct_bucket_bucket").desc_nulls_first())
)
