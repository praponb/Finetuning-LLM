from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")
w_recent = Window.partitionBy("city").orderBy("event_seq", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("city").orderBy(F.col("last3_max_sqft_cume_dist").desc_nulls_first())
w7 = Window.partitionBy("city").orderBy("last3_count_sqft_cume_dist", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.orderBy(F.col("sqft_cume_dist_avg_5").desc(), "id")
w9 = Window.partitionBy("city").orderBy(F.col("sqft_cume_dist_avg_5").desc(), "id")
w10 = Window.partitionBy("city").orderBy("last3_max_sqft_cume_dist", "id").rowsBetween(-2, -1)

scored = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        F.cume_dist().over(w).alias("sqft_cume_dist"),
        F.ntile(4).over(w2).alias("sqft_bucket"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left_semi")
)

with_windows = (
    kept
    .withColumn("last3_min_sqft_cume_dist", F.min("sqft_cume_dist").over(w_recent))
    .withColumn("last3_count_sqft_cume_dist", F.count("sqft_cume_dist").over(w_recent))
    .withColumn("last3_sum_sqft_cume_dist", F.sum("sqft_cume_dist").over(w_recent))
    .withColumn("last3_max_sqft_cume_dist", F.max("sqft_cume_dist").over(w_recent))
    .withColumn("sqft_cume_dist_avg_5", F.avg("sqft_cume_dist").over(w5))
)

enriched = (
    with_windows
    .filter(~F.col("city").isin(["Chiang Mai", "London"]))
    .select(
        "id",
        "city",
        "opened_date",
        "sqft_cume_dist",
        "last3_min_sqft_cume_dist",
        "last3_count_sqft_cume_dist",
        "last3_sum_sqft_cume_dist",
        F.percent_rank().over(w6).alias("last3_max_sqft_cume_dist_percent_rank"),
        F.avg("event_seq").over(w7).alias("event_seq_avg_2"),
        F.ntile(10).over(w8).alias("sqft_cume_dist_avg_5_bucket"),
        F.row_number().over(w9).alias("row_seq"),
        F.avg("sqft_bucket").over(w10).alias("sqft_bucket_avg_2_prior"),
    )
)

result = (
    enriched
    .groupBy("city")
    .agg(
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.max(F.when(F.col("city") == "Berlin", F.col("event_seq_avg_2"))).alias("berlin_max_event_seq_avg_2"),
    )
)
