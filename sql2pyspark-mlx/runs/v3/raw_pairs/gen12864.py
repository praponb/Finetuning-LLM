from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(153, 231))
    .agg(F.avg("quantity").alias("avg_quantity"))
)
