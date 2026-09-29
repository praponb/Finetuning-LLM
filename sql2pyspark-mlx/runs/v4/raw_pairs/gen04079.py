from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.product_name"))
)
