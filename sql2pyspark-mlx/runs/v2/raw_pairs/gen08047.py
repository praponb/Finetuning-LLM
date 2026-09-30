from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .select(
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
    )
    .orderBy(F.col("stock_qty").desc())
)
