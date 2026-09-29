from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.id", "a.product_id", "a.revenue", F.col("d.cost"), F.col("d.brand"))
)
