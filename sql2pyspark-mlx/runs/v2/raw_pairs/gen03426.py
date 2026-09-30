from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_sqft").desc())

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 15106, "high").when(F.col("sqft") >= 2653, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("sqft_rank", F.rank().over(w))
    .orderBy(F.col("sqft_bucket").desc())
)
