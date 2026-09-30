from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost_bucket")

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 101, "high").when(F.col("cost") >= 31, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
    .orderBy("cost_bucket")
)
