from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code", "promo_code").orderBy(F.col("promo_id").asc_nulls_first())
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy("promotion_promo_code")
w4 = Window.orderBy(F.col("starts_on").desc(), "id", "promo_id")
w5 = Window.partitionBy("promo_code").orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("promo_code").orderBy("event_seq", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("promo_id_share", "id", "promo_id").rowsBetween(-3, -1)
w8 = Window.orderBy(F.col("last7_sum_promo_id").desc(), "id", "promo_id")
w9 = Window.orderBy("event_seq", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy(F.col("event_seq").desc_nulls_last())
w12 = Window.orderBy(F.col("last7_avg_promo_id_avg_3_prior").asc_nulls_first())
w13 = Window.partitionBy("seq_no").orderBy(F.col("row_count").desc())

base = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-03-22").cast("date")) & F.col("channel").isNotNull())
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.36, "High").when(F.col("discount_pct") >= 0.18, "Medium").otherwise("Low"))
)

matched = (
    base
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("s.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    matched
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        F.dense_rank().over(w).alias("promo_id_dense_rank"),
        F.avg("discount_pct").over(w2).alias("promo_code_avg_discount_pct"),
        F.avg("discount_pct").over(w3).alias("promotion_promo_code_avg_discount_pct"),
        F.round((F.col("promo_id") / F.sum("promo_id").over(w3)), 4).alias("promo_id_share"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "promo_code_avg_discount_pct",
        "promotion_promo_code_avg_discount_pct",
        "promo_id_share",
        "event_seq",
        F.avg("promo_id").over(w5).alias("last7_avg_promo_id"),
        F.sum("promo_id").over(w5).alias("last7_sum_promo_id"),
        F.min("promo_id").over(w5).alias("last7_min_promo_id"),
        F.sum("promo_code_avg_discount_pct").over(w6).alias("cumulative_promo_code_avg_discount_pct"),
    )
)

with_windows = (
    metrics
    .withColumn("last7_avg_promo_id_avg_3_prior", F.avg("last7_avg_promo_id").over(w7))
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("first_known_promo_code_avg_discount_pct", F.first("promo_code_avg_discount_pct", ignorenulls=True).over(w9))
    .withColumn("seq_no_2", F.row_number().over(w8))
)

filtered = (
    with_windows
    .filter(F.col("promo_code_avg_discount_pct") >= 0)
    .select(
        "*",
        F.when(F.col("last7_min_promo_id") >= 10.5, "High").when(F.col("last7_min_promo_id") >= 5.3, "Medium").otherwise("Low").alias("last7_min_promo_id_level"),
    )
)

scored = (
    filtered
    .withColumn("running_promo_code_avg_discount_pct", F.sum("promo_code_avg_discount_pct").over(w10))
    .withColumn("event_seq_dense_rank", F.dense_rank().over(w11))
    .withColumn("last7_avg_promo_id_avg_3_prior_cume_dist", F.cume_dist().over(w12))
)

daily = (
    scored
    .groupBy(
        "seq_no",
        F.when(F.col("running_promo_code_avg_discount_pct") >= 1, "LARGE").otherwise("SMALL").alias("running_promo_code_avg_discount_pct_tier"),
    )
    .agg(
        F.max("promo_id").alias("max_promo_id"),
        F.min("promo_code_avg_discount_pct").alias("min_promo_code_avg_discount_pct"),
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("promotion_discount_pct", [0.25, 0.5, 0.75]).alias("promotion_discount_pct_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("row_count_dense_rank", F.dense_rank().over(w13))
)

result = (
    daily
    .select(
        "seq_no",
        "running_promo_code_avg_discount_pct_tier",
        "min_promo_code_avg_discount_pct",
        "row_count",
        "num_rows",
        "promotion_discount_pct_quartiles",
        "row_count_dense_rank",
    )
)
