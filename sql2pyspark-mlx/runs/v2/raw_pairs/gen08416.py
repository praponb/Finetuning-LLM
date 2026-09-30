from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.min_unit_price") >= 190)
    .select(
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("pt.min_unit_price"),
        F.col("pt.stddev_quantity"),
    )
    .orderBy("stock_qty")
)
