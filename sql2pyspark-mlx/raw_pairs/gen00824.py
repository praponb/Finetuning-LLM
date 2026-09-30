from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc_nulls_last())
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_last())
w5 = Window.partitionBy("order_id").orderBy("cost_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("cost_share", "id")
w7 = Window.partitionBy("order_id").orderBy("cost_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("order_id").orderBy("cost_percent_rank", "id")

valid = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-06-20").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.substring("carrier", 1, 2).alias("carrier_prefix"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "carrier_prefix",
    )
)

metrics = (
    deduped
    .filter(F.col("cost") < 70)
    .select(
        "id",
        "order_id",
        "cost",
        F.avg("cost").over(w2).alias("cost_avg_3"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
        F.percent_rank().over(w4).alias("cost_percent_rank"),
    )
)

metrics_shipments = (
    metrics
    .select(
        "*",
        F.sum("cost_avg_3").over(w5).alias("running_cost_avg_3"),
        F.ntile(4).over(w6).alias("cost_share_bucket"),
        F.last("cost_avg_3", ignorenulls=True).over(w7).alias("last_known_cost_avg_3"),
    )
)

valid_shipments = (
    metrics_shipments
    .filter(F.col("last_known_cost_avg_3") >= 66)
    .withColumn("running_cost_avg_3_band", F.when(F.col("running_cost_avg_3") >= 449, "High").when(F.col("running_cost_avg_3") >= 230, "Medium").otherwise("Low"))
)

with_windows = (
    valid_shipments
    .select(
        "id",
        "order_id",
        "cost",
        "cost_avg_3",
        "cost_percent_rank",
        "running_cost_avg_3",
        "cost_share_bucket",
        "last_known_cost_avg_3",
        "running_cost_avg_3_band",
        F.lead("cost_avg_3").over(w_running).alias("next_cost_avg_3"),
        F.lead("running_cost_avg_3").over(w_running).alias("next_running_cost_avg_3"),
        F.lag("running_cost_avg_3").over(w_running).alias("prev_running_cost_avg_3"),
        F.row_number().over(w_running).alias("event_seq"),
        F.lag("running_cost_avg_3", 1).over(w6).alias("prev_running_cost_avg_3_2"),
    )
)

result = (
    with_windows
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.channel").alias("channel"), F.col("d.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.next_running_cost_avg_3")).alias("max_next_running_cost_avg_3"),
        F.round(F.avg(F.col("e.prev_running_cost_avg_3_2")), 2).alias("avg_prev_running_cost_avg_3_2"),
    )
    .orderBy(F.col("avg_prev_running_cost_avg_3_2").desc_nulls_first())
)
