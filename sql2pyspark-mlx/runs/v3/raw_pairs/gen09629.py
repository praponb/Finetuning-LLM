from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_cost").desc())

result = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Umbrella", "Acme"]))
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cost_rank", F.dense_rank().over(w))
    .orderBy(F.col("cost_rank").desc())
)
