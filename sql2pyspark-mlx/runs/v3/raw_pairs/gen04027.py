from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 6]))
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(F.count("*").alias("num_inventory"))
    .filter(F.col("num_inventory") > 4)
    .orderBy(F.col("last_restock_year").desc())
)
