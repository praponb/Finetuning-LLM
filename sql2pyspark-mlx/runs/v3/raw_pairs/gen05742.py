from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("country")

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_rating", F.sum("total_rating").over(w))
    .orderBy(F.col("cnt").desc(), F.col("cumulative_rating").desc(), "country")
    .limit(3)
)
