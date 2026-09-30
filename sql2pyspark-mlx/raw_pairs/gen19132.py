from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code", "promo_code").orderBy("promotion_discount_pct", "id", "promo_id")
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("starts_on"), "promo_code").orderBy(F.col("promo_id").desc(), "id", "promo_id")
w_running = Window.partitionBy("promo_code").orderBy("row_seq", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.partitionBy("promo_code").orderBy("starts_on", "id", "promo_id").rowsBetween(-5, -1)
w6 = Window.partitionBy("promo_code").orderBy("row_seq", "id", "promo_id")
w7 = Window.partitionBy("promo_id").orderBy(F.col("welcome5_running_count_promo_id").desc())

filtered = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-10-09").cast("date"))
    .withColumn("discount_pct_whole", F.floor("discount_pct"))
)

matched = (
    filtered
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

latest = (
    matched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
    )
)

metrics = (
    latest
    .withColumn("running_min_promo_id", F.min("promo_id").over(w2))
    .withColumn("running_count_promo_id", F.count("promo_id").over(w2))
    .withColumn("running_sum_promo_id", F.sum("promo_id").over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

with_windows = (
    metrics
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_id",
        "running_min_promo_id",
        "running_count_promo_id",
        "running_sum_promo_id",
        F.sum("running_count_promo_id").over(w_running).alias("cumulative_running_count_promo_id"),
        F.avg("discount_pct").over(w_running_2).alias("discount_pct_avg_5_prior"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

result = (
    with_windows
    .groupBy(
        "promo_id",
        F.when(F.col("running_sum_promo_id") >= 37, "PREMIUM").otherwise("STANDARD").alias("running_sum_promo_id_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("discount_pct_avg_5_prior"))).alias("bundle_total_discount_pct_avg_5_prior"),
        F.percentile_approx("discount_pct_avg_5_prior", [0.25, 0.5, 0.75]).alias("discount_pct_avg_5_prior_quartiles"),
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_count"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("promo_code") == "VIP", 1)), F.lit(0)), 2).alias("seq_no_per_vip"),
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("running_count_promo_id")).otherwise(0)).alias("welcome5_running_count_promo_id"),
    )
    .withColumn("welcome5_running_count_promo_id_dense_rank", F.dense_rank().over(w7))
    .orderBy(F.col("bundle_total_discount_pct_avg_5_prior").asc_nulls_first())
)
