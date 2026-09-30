from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.stddev_unit_price") > 73)
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("pt.stddev_unit_price"),
    )
)
