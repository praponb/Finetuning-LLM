from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("unit_price") < 310)
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "inner")
    .filter(F.col("product_totals.quantity_range") > 27)
    .select(
        F.col("products.brand"),
        F.col("products.stock_qty"),
        F.col("product_totals.quantity_range"),
        F.col("product_totals.max_quantity"),
    )
)
