from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("cost", "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost_bucket").asc_nulls_last(), "id")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("status", "carrier")
w6 = Window.partitionBy("carrier", "status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("activity_month").orderBy(F.col("status_stddev_samp_cost_bucket_2_p95").desc())

metrics = (
    df_shipments
    .withColumn("cost_bucket", F.ntile(4).over(w_part))
    .withColumn("cost_bucket_2", F.ntile(3).over(w_part))
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

metrics_shipments = (
    deduped
    .select(
        "*",
        F.stddev_samp("cost_bucket_2").over(w3).alias("status_stddev_samp_cost_bucket_2"),
        F.ntile(4).over(w4).alias("cost_bucket_3"),
        F.min("cost_bucket").over(w5).alias("status_min_cost_bucket"),
        F.sum("cost_bucket").over(w6).alias("cumulative_cost_bucket"),
        F.sum("cost_bucket_2").over(w7).alias("cumulative_cost_bucket_2"),
    )
)

result = (
    metrics_shipments
    .groupBy(
        F.date_trunc("MONTH", "delivered_date").alias("activity_month"),
        "carrier",
        "status",
    )
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.percentile_approx("status_stddev_samp_cost_bucket_2", 0.95).alias("status_stddev_samp_cost_bucket_2_p95"),
    )
    .withColumn("status_stddev_samp_cost_bucket_2_p95_rank", F.rank().over(w8))
    .orderBy("carrier")
)
