from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity", "id")
w4 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy(F.col("seq_no").asc_nulls_first())
w6 = Window.partitionBy("order_status").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status")
w8 = Window.partitionBy("category")

metrics = (
    df_order_items
    .select(
        "*",
        F.corr("unit_price", "quantity").over(w).alias("corr_unit_price_quantity"),
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.row_number().over(w3).alias("seq_no"),
        F.last("quantity", ignorenulls=True).over(w4).alias("last_known_quantity"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
)

joined = (
    latest
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.amount"), F.col("d.status"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.launch_date"), F.col("d.stock_qty"))
)

joined_order_items = (
    with_dims
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.status").alias("order_status"),
        F.col("p.discount"),
        F.col("p.order_date"),
    )
)

enriched = (
    joined_order_items
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.category"),
        F.col("b.launch_date").alias("product_launch_date"),
        F.col("b.price"),
    )
)

result = (
    enriched
    .withColumn("first_known_running_quantity", F.first("running_quantity", ignorenulls=True).over(w6))
    .withColumn("status_sum_running_quantity", F.sum("running_quantity").over(w7))
    .withColumn("category_sum_corr_unit_price_quantity", F.sum("corr_unit_price_quantity").over(w8))
)
