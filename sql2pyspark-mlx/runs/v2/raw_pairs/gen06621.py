from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("city")

result = (
    df_stores
    .filter(F.col("opened_date").between("2024-04-01", "2024-12-01"))
    .groupBy("city")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_sqft", F.sum("total_sqft").over(w))
    .orderBy("city")
    .limit(1)
)
