from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") >= 84) & ((F.col("product_id") == 1) | (F.col("last_restock") >= "2023-04-03")))
    .orderBy("last_restock")
)
