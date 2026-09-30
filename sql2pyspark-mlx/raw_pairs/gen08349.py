from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy(F.date_trunc("month", "delivered_date")).orderBy("cost", "id")
w3 = Window.orderBy("cost", "id")
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("order_id").orderBy(F.col("cost_avg_2_prior").asc_nulls_last(), "id")
w6 = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_last(), "id")
w_hist = Window.partitionBy("order_id").orderBy("cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_shipments
    .filter(F.col("carrier") != "Kerry")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        F.avg("cost").over(w).alias("cost_avg_2_prior"),
        F.ntile(3).over(w2).alias("cost_bucket"),
        F.row_number().over(w3).alias("event_seq"),
        F.stddev_samp("cost").over(w4).alias("status_stddev_samp_cost"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "cost", "cost_bucket", "status_stddev_samp_cost")
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered = (
    ranked
    .filter(F.col("order_id") == 30)
    .select(
        "id",
        "order_id",
        "cost",
        "cost_bucket",
        "status_stddev_samp_cost",
        F.round((F.col("status_stddev_samp_cost") / F.col("cost")), 2).alias("status_stddev_samp_cost_per_cost"),
    )
)

metrics = (
    filtered
    .withColumn("running_min_cost_bucket", F.min("cost_bucket").over(w_hist))
    .withColumn("running_max_cost_bucket", F.max("cost_bucket").over(w_hist))
    .withColumn("running_count_cost_bucket", F.count("cost_bucket").over(w_hist))
    .withColumn("running_avg_cost_bucket", F.avg("cost_bucket").over(w_hist))
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w8))
)

result = (
    metrics
    .select("id", "order_id", "last_known_cost", "cost")
    .unpivot(
        ids=["id", "order_id"],
        values=["last_known_cost", "cost"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)
