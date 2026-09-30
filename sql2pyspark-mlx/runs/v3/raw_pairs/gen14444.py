from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_active")

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.sum("age").alias("total_age"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_age", F.sum("total_age").over(w))
)
