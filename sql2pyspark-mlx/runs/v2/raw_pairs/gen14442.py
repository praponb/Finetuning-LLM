from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("store_id")

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_revenue", F.sum("total_revenue").over(w))
    .orderBy(F.col("store_id").desc())
)
