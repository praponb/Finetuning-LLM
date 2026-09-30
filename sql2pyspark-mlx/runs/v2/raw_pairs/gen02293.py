from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("cost") < 700).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("i.product_id").isin([8, 10]))
    .select(F.col("i.id"), F.col("i.last_restock"), F.col("i.product_id"))
    .orderBy(F.col("id").desc())
)
