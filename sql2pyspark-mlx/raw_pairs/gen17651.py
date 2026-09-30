from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("stock_qty") == 351).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("i.last_restock") < "2023-08-01")
    .select(F.col("i.id"), F.col("i.quantity"), F.col("i.product_id"))
    .orderBy("quantity", "id")
)
