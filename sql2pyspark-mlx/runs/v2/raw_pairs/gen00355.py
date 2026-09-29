from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 2)
    .withColumn("quantity_whole", F.ceil("quantity"))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
    .orderBy("product_id", "id")
    .limit(5)
)
