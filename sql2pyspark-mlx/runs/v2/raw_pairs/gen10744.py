from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("carrier")

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2025-09-01")
    .groupBy("carrier")
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
)
