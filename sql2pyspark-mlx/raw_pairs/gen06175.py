from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy(F.year("delivered_date"), "carrier").orderBy("delivered_date", "id")
w3 = Window.partitionBy("order_id").orderBy("shipping_fee", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.orderBy("cost", "id")
w_running = Window.orderBy("shipping_fee", "id")
w6 = Window.partitionBy("order_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("channel").orderBy("next_shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_status", "channel").orderBy(F.col("next_shipping_fee").desc_nulls_last())

scored = (
    df_shipments
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("prev_cost", F.lag("cost", 1).over(w2))
)

with_dims = (
    scored
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.delivered_date",
        "e.cost",
        F.col("b.status").alias("order_status"),
        F.col("b.shipping_fee"),
        F.col("b.channel"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "cost",
        "order_status",
        "channel",
        F.avg("cost").over(w3).alias("cost_avg_2"),
        F.lead("shipping_fee", 1).over(w_hist).alias("next_shipping_fee"),
        F.ntile(3).over(w_running).alias("shipping_fee_bucket"),
        F.lag("cost", 1).over(w_hist).alias("prev_cost"),
        F.sum("cost").over(w6).alias("running_cost"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "cost",
        "cost_avg_2",
        "shipping_fee_bucket",
        "running_cost",
        F.sum("next_shipping_fee").over(w7).alias("running_sum_next_shipping_fee"),
        F.min("next_shipping_fee").over(w7).alias("running_min_next_shipping_fee"),
        F.avg("next_shipping_fee").over(w7).alias("running_avg_next_shipping_fee"),
        F.rank().over(w8).alias("next_shipping_fee_rank"),
    )
)

stats = (
    with_windows
    .withColumn("running_sum_next_shipping_fee_tier", F.when(F.col("running_sum_next_shipping_fee") >= 172, "LARGE").otherwise("SMALL"))
    .withColumn("running_min_next_shipping_fee_tier", F.when(F.col("running_min_next_shipping_fee") >= 44, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["running_sum_next_shipping_fee_tier", "running_min_next_shipping_fee_tier"], ["running_sum_next_shipping_fee_tier"], []],
        "running_sum_next_shipping_fee_tier",
        "running_min_next_shipping_fee_tier",
    )
    .agg(
        F.round(F.avg("running_min_next_shipping_fee"), 2).alias("avg_running_min_next_shipping_fee"),
        F.percentile_approx("next_shipping_fee_rank", 0.5).alias("next_shipping_fee_rank_p50"),
    )
)

result = (
    stats
    .select("running_min_next_shipping_fee_tier", "next_shipping_fee_rank_p50")
    .orderBy(F.col("running_min_next_shipping_fee_tier").desc_nulls_last())
)
