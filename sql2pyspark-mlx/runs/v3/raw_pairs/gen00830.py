from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left_anti")
    .filter(F.col("p.brand").isNotNull())
    .select(F.col("p.id"), F.col("p.price"), F.col("p.category"))
    .orderBy("category", F.col("price").desc(), "id")
    .limit(20)
)
