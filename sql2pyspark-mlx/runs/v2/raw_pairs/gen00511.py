from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_month")

result = (
    df_products
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
    .orderBy("launch_month", F.col("cumulative_cost").desc())
)
