from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.col("s.opened_date"),
    )
    .orderBy("cost", "last_restock")
)
