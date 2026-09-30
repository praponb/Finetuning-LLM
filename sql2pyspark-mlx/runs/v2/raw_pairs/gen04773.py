from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", F.year("last_restock").alias("last_restock_year"))
    .agg(F.min("quantity").alias("min_quantity"))
)
