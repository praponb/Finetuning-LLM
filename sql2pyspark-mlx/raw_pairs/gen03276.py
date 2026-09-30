from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("city").orderBy("rn")
w3 = Window.partitionBy("store_id").orderBy(F.col("sqft").desc_nulls_first())
w4 = Window.partitionBy("store_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("sqft").desc())
w6 = Window.partitionBy(F.date_trunc("week", "opened_date"), "store_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy(F.col("sqft").desc_nulls_last(), "id")
w_recent = Window.orderBy("cumulative_sqft", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy(F.month("opened_date"))
w10 = Window.orderBy("last3_avg_sqft", "id").rowsBetween(-3, Window.currentRow)

joined = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.sqft"), F.col("s.opened_date"), F.col("s.city"))
)

ranked = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

deduped = (
    ranked
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "sqft", "opened_date")
)

enriched = (
    deduped
    .select(
        "*",
        F.cume_dist().over(w3).alias("sqft_cume_dist"),
        F.sum("sqft").over(w4).alias("cumulative_sqft"),
        F.rank().over(w5).alias("sqft_rank"),
        F.sum("sqft").over(w6).alias("cumulative_sqft_2"),
    )
)

ranked_store_sales = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "sqft",
        "opened_date",
        "cumulative_sqft",
        "sqft_rank",
        "cumulative_sqft_2",
    )
)

metrics = (
    ranked_store_sales
    .filter(F.col("cumulative_sqft_2") >= 76200)
    .withColumn("last3_sum_sqft", F.sum("sqft").over(w_recent))
    .withColumn("last3_min_sqft", F.min("sqft").over(w_recent))
    .withColumn("last3_avg_sqft", F.avg("sqft").over(w_recent))
    .withColumn("opened_date_month_min_cumulative_sqft", F.min("cumulative_sqft").over(w_ordered))
)

with_windows = (
    metrics
    .withColumn("last4_count_last3_avg_sqft", F.count("last3_avg_sqft").over(w10))
    .withColumn("last4_min_last3_avg_sqft", F.min("last3_avg_sqft").over(w10))
    .withColumn("last4_max_last3_avg_sqft", F.max("last3_avg_sqft").over(w10))
    .withColumn("last4_avg_last3_avg_sqft", F.avg("last3_avg_sqft").over(w10))
)

result = (
    with_windows
    .cube(
        F.when(F.col("cumulative_sqft") >= 87934, "PREMIUM").otherwise("STANDARD").alias("cumulative_sqft_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_sqft", 0.95).alias("cumulative_sqft_p95"),
        F.percentile_approx("last3_min_sqft", [0.25, 0.5, 0.75]).alias("last3_min_sqft_quartiles"),
        F.percentile_approx("last4_max_last3_avg_sqft", 0.5).alias("last4_max_last3_avg_sqft_p50"),
        F.avg("last4_min_last3_avg_sqft").alias("avg_last4_min_last3_avg_sqft"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)
