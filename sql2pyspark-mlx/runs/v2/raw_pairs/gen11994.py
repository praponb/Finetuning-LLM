from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("country")

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("cumulative_rating", F.sum("total_rating").over(w))
    .orderBy(F.col("total_rating").desc())
)
