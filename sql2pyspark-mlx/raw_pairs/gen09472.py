from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("line_total", "id")
w2 = Window.partitionBy("order_id")
w3 = Window.orderBy("unit_price", "id")
w4 = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("order_id", "product_id").orderBy(F.col("line_total_bucket").desc_nulls_first(), "id")
w6 = Window.orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("line_total", "id")
w8 = Window.partitionBy("channel").orderBy(F.col("amount").desc_nulls_last(), "id")
w9 = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(-6, Window.currentRow)
w_part = Window.orderBy("event_seq", "id")
w_ordered = Window.partitionBy("channel").orderBy("cumulative_discount", "id")
w12 = Window.partitionBy("channel").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("channel").orderBy("line_total")

cleaned = (
    df_order_items
    .filter(F.col("unit_price") <= 190)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

with_windows = (
    cleaned
    .withColumn("line_total_bucket", F.ntile(3).over(w))
    .withColumn("unit_price_share", F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4))
    .withColumn("next_line_total", F.lead("line_total", 1, 0).over(w3))
    .withColumn("unit_price_avg_5_prior", F.avg("unit_price").over(w4))
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    ranked
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.line_total",
        "e.next_line_total",
        F.col("o.amount"),
        F.col("o.channel"),
        F.col("o.discount"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.sum("discount").over(w6).alias("cumulative_discount"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

ranked_order_items = (
    scored
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    ranked_order_items
    .withColumn("discount_avg_6", F.avg("discount").over(w9))
    .withColumn("event_seq_bucket", F.ntile(10).over(w_part))
    .withColumn("cumulative_discount_bucket", F.ntile(4).over(w_ordered))
    .withColumn("last_known_cumulative_discount", F.last("cumulative_discount", ignorenulls=True).over(w12))
    .withColumn("line_total_percent_rank", F.percent_rank().over(w13))
)

result = (
    metrics
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["status", "order_channel"], ["status"]],
        F.col("p.status").alias("status"),
        F.col("p.channel").alias("order_channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.event_seq")).alias("max_event_seq"),
    )
)
