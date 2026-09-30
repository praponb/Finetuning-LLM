from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_sqft").desc())

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12243, "high").when(F.col("sqft") >= 8115, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("sqft_rank", F.rank().over(w))
    .orderBy("total_sqft", "sqft_bucket")
    .limit(5)
)
