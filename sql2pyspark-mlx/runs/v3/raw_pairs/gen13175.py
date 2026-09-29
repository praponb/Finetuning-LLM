from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft_bucket")

result = (
    df_stores
    .filter(F.col("sqft") >= 7876)
    .groupBy(
        F.when(F.col("sqft") >= 15710, "high").when(F.col("sqft") >= 7651, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("cumulative_sqft", F.sum("total_sqft").over(w))
    .orderBy("sqft_bucket", F.col("cumulative_sqft").desc())
    .limit(50)
)
