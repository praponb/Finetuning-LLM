from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promo_code", "channel")
w3 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id")
w_running = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("discount_pct_share").desc(), "id")

valid = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-11-12").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.trim("promo_code").alias("clean_promo_code"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "clean_promo_code",
        F.ntile(4).over(w).alias("discount_pct_bucket"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "ends_on",
        "discount_pct_share",
        "event_seq",
        F.avg("discount_pct_share").over(w_running).alias("running_avg_discount_pct_share"),
        F.min("discount_pct_share").over(w_running).alias("running_min_discount_pct_share"),
        F.max("discount_pct_share").over(w_running).alias("running_max_discount_pct_share"),
        F.ntile(4).over(w5).alias("discount_pct_share_bucket"),
    )
)

cleaned = (
    metrics
    .filter(F.col("discount_pct_share") >= 0.56)
    .withColumn("least_discount_pct_share_running_min_discount_pct_share", F.least("discount_pct_share", "running_min_discount_pct_share"))
)

cleaned_promotions = (
    cleaned
    .filter(F.col("event_seq") < 7)
    .withColumn("years_since_ends_on", (F.datediff(F.current_date(), "ends_on") / 365))
)

grouped = (
    cleaned_promotions
    .rollup(
        F.when(F.col("discount_pct_share") >= 0.82, "HIGH").otherwise("LOW").alias("discount_pct_share_tier"),
        F.date_trunc("quarter", "ends_on").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("years_since_ends_on", 0.95).alias("years_since_ends_on_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count("*").alias("cnt"),
        F.avg("years_since_ends_on").alias("avg_years_since_ends_on"),
    )
    .filter(F.col("distinct_ids_2") > 1)
)

result = (
    grouped
    .select(
        "years_since_ends_on_p95",
        "distinct_ids_2",
        "cnt",
        "avg_years_since_ends_on",
    )
    .orderBy(F.col("distinct_ids_2").desc(), "cnt")
)
