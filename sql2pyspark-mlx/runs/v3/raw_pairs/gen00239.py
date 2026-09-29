from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_quarter")

result = (
    df_shipments
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
    .orderBy("cumulative_cost", "shipped_quarter")
)
