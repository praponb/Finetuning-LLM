from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.month(F.col("i.last_restock")).isin([1, 10, 12]) | F.col("i.product_id").isin([5, 9, 10]))
    .select(F.col("p.stock_qty"), F.col("i.product_id"), F.col("i.quantity"))
    .orderBy(F.col("product_id").desc())
)
