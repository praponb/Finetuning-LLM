from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_rating").desc())

result = (
    df_suppliers
    .groupBy("city")
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("rnk", F.rank().over(w))
)
