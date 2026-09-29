from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("store_id")

result = (
    df_store_sales
    .filter(F.col("revenue").between(1100, 1400))
    .groupBy("store_id")
    .agg(F.sum("units").alias("total_units"))
    .withColumn("cumulative_units", F.sum("total_units").over(w))
    .orderBy("total_units")
)
