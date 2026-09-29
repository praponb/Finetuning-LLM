from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.last_restock") >= "2024-08-01")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
    )
)
