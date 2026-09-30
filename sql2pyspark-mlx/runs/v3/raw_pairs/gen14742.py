from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_revenue").desc())

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 6]))
    .groupBy("store_id")
    .agg(F.sum("revenue").alias("total_revenue"))
    .withColumn("rnk", F.rank().over(w))
)
