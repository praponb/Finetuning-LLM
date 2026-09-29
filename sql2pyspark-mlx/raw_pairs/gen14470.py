from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("shipped_date"), "carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "order_id")
w4 = Window.partitionBy("status", "carrier")
w5 = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_last())
w6 = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")
w7 = Window.partitionBy(F.date_trunc("month", "shipped_date"), "carrier").orderBy("cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("carrier").orderBy("cost_rank", "id")
w9 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("carrier").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-04-24").cast("date"))
    .select(
        "*",
        F.date_format("delivered_date", "MMM yyyy").alias("delivered_date_label"),
    )
)

enriched = (
    filtered
    .filter(F.col("shipped_date") < "2023-07-01")
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "delivered_date_label",
        F.ntile(10).over(w).alias("cost_bucket"),
        F.first("cost", ignorenulls=True).over(w2).alias("first_known_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
        F.round((F.col("cost") / F.sum("cost").over(w4)), 4).alias("cost_share_2"),
        F.rank().over(w5).alias("cost_rank"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.row_number().over(w6).alias("event_seq"),
        F.sum("cost_share_2").over(w7).alias("running_cost_share_2"),
    )
)

metrics_shipments = (
    metrics
    .withColumn("prev_cost_rank", F.lag("cost_rank").over(w_ordered))
    .withColumn("prev_running_cost_share_2", F.lag("running_cost_share_2").over(w_ordered))
    .withColumn("next_running_cost_share_2", F.lead("running_cost_share_2").over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("cumulative_cost_bucket", F.sum("cost_bucket").over(w9))
)

with_windows = (
    metrics_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date_label",
        "cost_bucket",
        "cost_share_2",
        "cost_rank",
        "event_seq",
        "next_running_cost_share_2",
        "cumulative_cost_bucket",
        F.min("cost_share").over(w10).alias("running_min_cost_share"),
        F.max("cost_share").over(w10).alias("running_max_cost_share"),
        F.avg("cost_share").over(w10).alias("running_avg_cost_share"),
        F.sum("cost_share").over(w10).alias("running_sum_cost_share"),
    )
)

grouped = (
    with_windows
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("activity_month"), "carrier")
    .agg(
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.round(F.avg("running_avg_cost_share"), 2).alias("avg_running_avg_cost_share"),
        F.round(F.avg("running_max_cost_share"), 2).alias("avg_running_max_cost_share"),
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_rows"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("next_running_cost_share_2"))).alias("fedex_total_next_running_cost_share_2"),
    )
)

result = (
    grouped
    .select(
        "activity_month",
        "carrier",
        "distinct_carriers",
        "avg_running_avg_cost_share",
    )
    .orderBy(F.col("carrier").asc_nulls_first())
)
