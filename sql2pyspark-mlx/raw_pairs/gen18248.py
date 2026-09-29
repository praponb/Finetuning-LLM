from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("price").asc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("stock_qty", "id")
w3 = Window.partitionBy("product_id", "order_id").orderBy("stock_qty", "id")
w4 = Window.orderBy(F.col("stock_qty").desc_nulls_last())
w5 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("last_known_stock_qty_plus_next_stock_qty", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", F.round((F.col("unit_price") / F.col("quantity")), 2))
    .withColumn("line_total_2", (F.col("quantity") - F.col("unit_price")))
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.line_total",
        F.col("p.price"),
        F.col("p.stock_qty"),
    )
)

deduped = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    deduped
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "line_total",
        "price",
        F.lead("stock_qty", 1).over(w2).alias("next_stock_qty"),
        F.ntile(5).over(w3).alias("stock_qty_bucket"),
        F.cume_dist().over(w4).alias("stock_qty_cume_dist"),
        F.last("stock_qty", ignorenulls=True).over(w5).alias("last_known_stock_qty"),
    )
)

filtered = (
    metrics
    .filter(F.col("last_known_stock_qty") < 103)
    .select(
        "id",
        "line_total",
        "price",
        "next_stock_qty",
        "stock_qty_bucket",
        "stock_qty_cume_dist",
        "last_known_stock_qty",
        (F.col("next_stock_qty") + F.col("last_known_stock_qty")).alias("next_stock_qty_plus_last_known_stock_qty"),
        (F.col("last_known_stock_qty") + F.col("next_stock_qty")).alias("last_known_stock_qty_plus_next_stock_qty"),
    )
)

metrics_order_items = (
    filtered
    .withColumn("prev_next_stock_qty_plus_last_known_stock_qty", F.lag("next_stock_qty_plus_last_known_stock_qty").over(w6))
    .withColumn("next_next_stock_qty_plus_last_known_stock_qty", F.lead("next_stock_qty_plus_last_known_stock_qty").over(w6))
    .withColumn("prev_stock_qty_cume_dist", F.lag("stock_qty_cume_dist").over(w6))
    .withColumn("seq_no", F.row_number().over(w6))
)

result = (
    metrics_order_items
    .select(
        "id",
        "line_total",
        "price",
        "stock_qty_cume_dist",
        "last_known_stock_qty",
        "next_stock_qty_plus_last_known_stock_qty",
        "last_known_stock_qty_plus_next_stock_qty",
        "prev_next_stock_qty_plus_last_known_stock_qty",
        "prev_stock_qty_cume_dist",
    )
    .orderBy(
        F.col("next_stock_qty_plus_last_known_stock_qty").asc_nulls_last(),
        F.col("last_known_stock_qty_plus_next_stock_qty").desc(),
    )
)
