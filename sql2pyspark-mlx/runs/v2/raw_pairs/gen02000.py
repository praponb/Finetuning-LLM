from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id", "product_id", F.floor("quantity").alias("quantity_whole"))
    .orderBy("product_id")
)
