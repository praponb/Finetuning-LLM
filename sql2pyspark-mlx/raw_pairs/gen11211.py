from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("ends_on", "id")
w_part = Window.orderBy("seq_no", "id")
w5 = Window.orderBy(F.col("running_discount_pct").desc(), "id")
w6 = Window.orderBy(F.col("event_seq").desc())

deduped = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "starts_on", "ends_on", "discount_pct")
)

scored = (
    deduped
    .filter(F.col("discount_pct").between(0.2, 0.29))
    .select(
        "*",
        F.sum("discount_pct").over(w2).alias("running_discount_pct"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

scored_promotions = (
    scored
    .select(
        "id",
        "starts_on",
        "ends_on",
        "running_discount_pct",
        F.lead("running_discount_pct").over(w_part).alias("next_running_discount_pct"),
        F.lag("running_discount_pct").over(w_part).alias("prev_running_discount_pct"),
        F.lag("discount_pct").over(w_part).alias("prev_discount_pct"),
        F.lead("discount_pct").over(w_part).alias("next_discount_pct"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

enriched = (
    scored_promotions
    .select(
        "*",
        F.ntile(3).over(w5).alias("running_discount_pct_bucket"),
        F.cume_dist().over(w6).alias("event_seq_cume_dist"),
    )
)

base = (
    enriched
    .filter((F.col("ends_on") >= F.lit("2023-08-17").cast("date")) & F.col("prev_discount_pct").between(0.12, 0.28))
    .select(
        "id",
        "ends_on",
        "running_discount_pct",
        "next_running_discount_pct",
        "prev_running_discount_pct",
        "prev_discount_pct",
        "next_discount_pct",
        "event_seq",
        "running_discount_pct_bucket",
        "event_seq_cume_dist",
        (F.col("event_seq_cume_dist") - F.col("running_discount_pct_bucket")).alias("event_seq_cume_dist_minus_running_discount_pct_bucket"),
    )
)

base_promotions = (
    base
    .filter(F.col("ends_on") > F.to_date(F.lit("2023-07-09")))
    .select(
        "*",
        F.round((F.col("prev_running_discount_pct") / F.col("next_running_discount_pct")), 2).alias("prev_running_discount_pct_per_next_running_discount_pct"),
    )
)

result = (
    base_promotions
    .groupBy(
        F.when(F.col("next_discount_pct") >= 0.29, "LARGE").otherwise("SMALL").alias("next_discount_pct_tier"),
    )
    .agg(
        F.round(F.avg("next_running_discount_pct"), 2).alias("avg_next_running_discount_pct"),
        F.round(F.avg("prev_running_discount_pct_per_next_running_discount_pct"), 2).alias("avg_prev_running_discount_pct_per_next_running_discount_pct"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_discount_pct_bucket", 0.95).alias("running_discount_pct_bucket_p95"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("distinct_ids_2").desc())
)
