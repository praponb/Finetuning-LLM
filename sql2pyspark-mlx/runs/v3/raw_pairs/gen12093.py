from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating_bucket")

result = (
    df_suppliers
    .filter(~F.col("country").isin(["JP", "SG", "US"]))
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_rating", F.sum("total_rating").over(w))
    .orderBy("total_rating", "cumulative_rating")
)
