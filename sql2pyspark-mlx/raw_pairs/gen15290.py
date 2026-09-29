from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_order_items
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .filter(F.array_contains(F.col("d.tags"), "new"))
    .select(
        "e.id",
        "e.product_id",
        "e.quantity",
        F.col("d.category"),
        F.col("d.stock_qty"),
    )
)

cleaned = (
    with_dims
    .filter(F.col("stock_qty") < 101)
    .select(
        "id",
        "product_id",
        "category",
        "stock_qty",
        F.coalesce("category", F.lit("Unknown")).alias("category_filled"),
        F.coalesce("category", F.lit("N/A")).alias("category_clean"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "product_id",
        F.max("stock_qty").over(w_part).alias("running_max_stock_qty"),
        F.count("stock_qty").over(w_part).alias("running_count_stock_qty"),
        F.min("stock_qty").over(w_part).alias("running_min_stock_qty"),
        F.sum("stock_qty").over(w_part).alias("running_sum_stock_qty"),
        F.sum("stock_qty").over(w2).alias("running_stock_qty"),
    )
)

with_dims_order_items = (
    enriched
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.running_count_stock_qty",
        "src.running_stock_qty",
        F.col("d.brand"),
        F.col("d.price"),
        F.col("d.stock_qty"),
    )
)

result = (
    with_dims_order_items
    .select(
        "id",
        "product_id",
        "running_count_stock_qty",
        "running_stock_qty",
        "brand",
        "price",
    )
    .orderBy(F.col("price").asc_nulls_last())
)
