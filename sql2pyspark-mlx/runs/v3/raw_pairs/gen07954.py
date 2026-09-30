from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category"))
    .agg(F.min(F.col("i.quantity")).alias("min_quantity"))
    .orderBy("category")
)
