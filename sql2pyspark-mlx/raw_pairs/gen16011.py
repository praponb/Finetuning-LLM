from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 6]) | (F.col("last_restock") > "2023-12-11"))
    .withColumn("quantity_num", F.col("quantity").cast("double"))
    .orderBy(F.col("id").desc(), "quantity")
)
