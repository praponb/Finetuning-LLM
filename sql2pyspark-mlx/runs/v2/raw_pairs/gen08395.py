from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("city")

result = (
    df_suppliers
    .filter(F.col("rating") < 3.5)
    .groupBy("city")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_rating", F.sum("total_rating").over(w))
)
