from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("stock_qty") >= 162).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .select(F.col("i.id"), F.col("i.quantity"), F.col("i.product_id"))
    .orderBy("product_id", F.col("quantity").desc(), "id")
    .limit(20)
)
