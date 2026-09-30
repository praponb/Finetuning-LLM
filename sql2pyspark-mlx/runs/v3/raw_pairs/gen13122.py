from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.col("pt.min_quantity"),
        F.col("pt.quantity_range"),
    )
)
