from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("launch_date"), "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "product_id")
w3 = Window.orderBy("launch_date", "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("event_seq").desc(), "id")
w5 = Window.orderBy("stock_qty", "id").rowsBetween(-3, -1)
w6 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.launch_date"), F.col("d.stock_qty"), F.col("d.price"))
)

scored = (
    joined
    .filter(F.col("quantity_whole") <= 20)
    .select(
        "*",
        F.sum("stock_qty").over(w).alias("running_stock_qty"),
        F.sum("quantity_whole").over(w2).alias("order_id_sum_quantity_whole"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "order_id",
        "quantity",
        "quantity_whole",
        "launch_date",
        "running_stock_qty",
        "event_seq",
        F.ntile(5).over(w4).alias("event_seq_bucket"),
        F.avg("order_id_sum_quantity_whole").over(w5).alias("order_id_sum_quantity_whole_avg_3_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "quantity_whole",
        "running_stock_qty",
        "event_seq_bucket",
        "order_id_sum_quantity_whole_avg_3_prior",
        F.max("event_seq").over(w6).alias("running_max_event_seq"),
        F.min("event_seq").over(w6).alias("running_min_event_seq"),
        F.avg("event_seq").over(w6).alias("running_avg_event_seq"),
        F.row_number().over(w7).alias("row_seq"),
    )
)
