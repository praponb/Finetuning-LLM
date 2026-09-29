from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
    .orderBy(F.col("max_quantity").desc(), "category")
    .limit(50)
)
