from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("p.stock_qty") == 395)
    .select(
        F.col("p.stock_qty"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.datediff(F.current_date(), F.col("i.last_restock")).alias("days_since_last_restock"),
    )
)
