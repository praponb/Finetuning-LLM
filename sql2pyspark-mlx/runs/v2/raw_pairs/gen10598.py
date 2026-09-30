from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .select(
        F.col("p.stock_qty"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.date_add(F.col("p.launch_date"), 31).alias("launch_date_plus_31d"),
    )
    .orderBy("stock_qty", F.col("launch_date_plus_31d").desc())
)
