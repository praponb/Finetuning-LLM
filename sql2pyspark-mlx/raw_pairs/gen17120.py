from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.month(F.col("i.last_restock")).alias("last_restock_month"),
    )
    .orderBy(F.col("stock_qty").desc(), "product_name")
)
