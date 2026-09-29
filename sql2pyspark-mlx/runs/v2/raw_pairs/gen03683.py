from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_rating").desc())

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "DE", "SG"]))
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("total_rating").desc())
)
