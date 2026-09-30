from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("category").isin(["Sports", "Books", "Grocery"])).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_semi")
    .select(F.col("i.id"), F.col("i.last_restock"))
)
