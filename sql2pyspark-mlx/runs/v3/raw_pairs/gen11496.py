from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~((F.col("last_restock") > "2024-01-01") & (F.col("product_id") == 5)))
    .select("product_id", "quantity", F.round("quantity", 0).alias("quantity_rounded"))
)
