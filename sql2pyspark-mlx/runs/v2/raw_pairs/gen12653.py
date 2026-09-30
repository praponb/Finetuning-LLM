from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)
