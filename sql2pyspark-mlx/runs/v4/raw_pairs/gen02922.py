from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("singapore_count").desc())

result = (
    df_customers
    .groupBy("city", F.date_trunc("quarter", "signup_date").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", F.col("age")).otherwise(0)).alias("consumer_age"),
        F.max("age").alias("max_age"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.count("*").alias("cnt"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("age_per_bangkok"),
    )
    .filter(F.col("max_age") < 0)
    .withColumn("singapore_count_dense_rank", F.dense_rank().over(w))
)
