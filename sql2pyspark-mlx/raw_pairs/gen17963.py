from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy(F.year("order_date"), "order_id").orderBy(F.col("discount").desc_nulls_first())
w4 = Window.partitionBy("order_id").orderBy("line_total", "id")
w5 = Window.partitionBy("product_id", "order_id").orderBy("discount", "id").rowsBetween(-6, Window.currentRow)
w_ordered = Window.partitionBy("product_id").orderBy("product_id_avg_line_total", "id")
w7 = Window.partitionBy(F.date_trunc("MONTH", "order_date"), "product_id").orderBy(F.col("discount").desc(), "id")
w8 = Window.partitionBy("order_id").orderBy("product_id_avg_line_total", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("order_id").orderBy("next_line_total", "id")
w10 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("order_id").orderBy("discount_avg_6", "id").rowsBetween(-6, -1)

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([1, 8, 17, 29]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
        F.ceil("unit_price").alias("unit_price_whole"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.product_id",
        "e.line_total",
        F.col("d.amount"),
        F.col("d.order_date"),
        F.col("d.discount"),
    )
)

metrics = (
    with_dims
    .filter(F.col("discount").between(0.12, 0.4))
    .withColumn("product_id_avg_line_total", F.avg("line_total").over(w))
    .withColumn("running_discount", F.sum("discount").over(w2))
    .withColumn("discount_percent_rank", F.percent_rank().over(w_running))
    .withColumn("next_line_total", F.lead("line_total", 2).over(w4))
)

metrics_order_items = (
    metrics
    .select(
        "*",
        F.avg("discount").over(w5).alias("discount_avg_6"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.ntile(10).over(w7).alias("discount_bucket"),
    )
)

base = (
    metrics_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "line_total",
        "amount",
        "discount",
        "product_id_avg_line_total",
        "running_discount",
        "next_line_total",
        "discount_avg_6",
        "row_seq",
        "discount_bucket",
        F.date_add("order_date", 43).alias("order_date_plus_43d"),
    )
)

enriched = (
    base
    .select(
        "id",
        "line_total",
        "amount",
        "product_id_avg_line_total",
        "next_line_total",
        "discount_avg_6",
        "discount_bucket",
        F.avg("next_line_total").over(w8).alias("next_line_total_avg_6"),
        F.row_number().over(w9).alias("row_seq_2"),
        F.first("discount_avg_6", ignorenulls=True).over(w10).alias("first_known_discount_avg_6"),
        F.sum("discount_avg_6").over(w11).alias("cumulative_discount_avg_6"),
        F.avg("running_discount").over(w12).alias("running_discount_avg_6_prior"),
    )
)

result = (
    enriched
    .select(
        "id",
        "line_total",
        "amount",
        "product_id_avg_line_total",
        "next_line_total",
        "discount_avg_6",
        "discount_bucket",
        "next_line_total_avg_6",
        "row_seq_2",
    )
)
