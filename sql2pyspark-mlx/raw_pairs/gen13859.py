from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.last_restock") > "2023-11-25")
    .select(
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("p.brand"),
    )
)
