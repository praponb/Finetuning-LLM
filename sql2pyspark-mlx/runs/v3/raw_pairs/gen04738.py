from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(148, 218) & (F.col("last_restock") < "2024-07-01"))
    .groupBy("store_id", F.year("last_restock").alias("last_restock_year"))
    .agg(F.count("*").alias("num_inventory"))
)
