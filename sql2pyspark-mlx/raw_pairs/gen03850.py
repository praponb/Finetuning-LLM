from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_revenue").desc())

result = (
    df_store_sales
    .filter(F.col("revenue").between(3200, 3700))
    .groupBy("store_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("cnt"),
    )
    .withColumn("revenue_rank", F.rank().over(w))
    .orderBy(F.col("cnt").desc(), "store_id")
)
