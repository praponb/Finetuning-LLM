from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_rating").desc())

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("rnk").desc())
)
