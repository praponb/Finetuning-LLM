from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("carrier")

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
    .orderBy("carrier")
)
