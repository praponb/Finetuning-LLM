from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.length("product_name") < 10).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("i.last_restock") >= "2024-04-16")
    .select(F.col("i.id"), F.col("i.quantity"))
    .orderBy("id")
    .limit(50)
)
