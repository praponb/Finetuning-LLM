from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_quarter")

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([8, 10, 12]))
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.sum("revenue").alias("total_revenue"))
    .withColumn("cumulative_revenue", F.sum("total_revenue").over(w))
    .orderBy(F.col("sale_quarter").desc())
    .limit(5)
)
