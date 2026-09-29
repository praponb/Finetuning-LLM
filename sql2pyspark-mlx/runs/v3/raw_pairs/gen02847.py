from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("region")

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_sqft", F.sum("total_sqft").over(w))
)
