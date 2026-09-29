from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("p.stock_qty").between(358, 374) & F.col("i.quantity").isNotNull())
    .select(F.col("p.stock_qty"), F.col("i.product_id"), F.col("i.last_restock"))
    .orderBy(F.col("stock_qty").desc(), "last_restock")
)
