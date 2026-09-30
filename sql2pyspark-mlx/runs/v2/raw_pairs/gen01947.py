from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_revenue").desc())

result = (
    df_store_sales
    .filter(F.col("revenue").between(1600, 3500))
    .groupBy("store_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("rnk").desc())
)
