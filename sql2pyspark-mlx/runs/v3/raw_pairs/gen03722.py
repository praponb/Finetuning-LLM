from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating_bucket")

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("cumulative_rating", F.sum("total_rating").over(w))
    .orderBy("cumulative_rating", "rating_bucket")
    .limit(5)
)
