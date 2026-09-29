from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 5]))
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.quantity_range") > 44)
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("pt.quantity_range"),
        F.col("pt.last_last_restock"),
    )
    .orderBy("stock_qty")
)
