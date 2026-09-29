from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("stock_qty") != 399).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("i.quantity").isNotNull())
    .select(F.col("i.id"), F.col("i.quantity"), F.col("i.store_id"))
    .orderBy(F.col("quantity").desc())
)
