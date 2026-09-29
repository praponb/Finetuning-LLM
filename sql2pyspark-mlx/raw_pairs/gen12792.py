from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.orderBy("line_total", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("stock_qty").desc(), "id")
w4 = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(-3, -1)
w_hist = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("order_id").isin([38, 40]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.price").between(630, 970))
    .select("a.*", F.col("d.product_name"))
)

joined_order_items = (
    joined
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.line_total",
        "s.product_name",
        F.col("p.price"),
        F.col("p.product_name").alias("product_product_name"),
        F.col("p.launch_date"),
    )
)

enriched = (
    joined_order_items
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.line_total",
        "e.price",
        F.col("b.category"),
        F.col("b.stock_qty"),
        F.col("b.launch_date").alias("product_launch_date"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "product_id", "line_total", "category", "stock_qty")
)

result = (
    ranked
    .select(
        "id",
        "product_id",
        "line_total",
        "category",
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
        F.avg("stock_qty").over(w4).alias("stock_qty_avg_3_prior"),
        F.sum("stock_qty").over(w_hist).alias("cumulative_stock_qty"),
    )
)
