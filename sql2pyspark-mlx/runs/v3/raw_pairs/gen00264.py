from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.year("launch_date") == 2025).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .select(F.col("i.id"), F.col("i.last_restock"))
    .orderBy(F.col("last_restock").desc(), F.col("id").desc())
)
