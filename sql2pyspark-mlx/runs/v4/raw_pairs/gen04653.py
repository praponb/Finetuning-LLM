from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("store_id").isin([5, 6]) | (F.col("quantity") == 120))
    .withColumn("quantity_num", F.col("quantity").cast("double"))
)

agg = (
    recent_inventory
    .groupBy("store_id")
    .agg(F.min("quantity_num").alias("min_quantity_num"))
)

result = agg.filter(F.col("min_quantity_num") < 27)
