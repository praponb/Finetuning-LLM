from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_rating").desc())

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .groupBy("city")
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("rating_rank", F.dense_rank().over(w))
    .orderBy(F.col("total_rating").desc())
)
