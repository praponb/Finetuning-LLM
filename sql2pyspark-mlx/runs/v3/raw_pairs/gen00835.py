from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_age").desc())

result = (
    df_customers
    .filter(F.col("country").isin(["TH", "US", "DE"]))
    .groupBy(
        F.when(F.col("age") >= 57, "high").when(F.col("age") >= 42, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.sum("age").alias("total_age"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("total_age").desc())
)
