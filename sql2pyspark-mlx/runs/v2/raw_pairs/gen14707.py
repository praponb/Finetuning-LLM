from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_age").desc())

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .groupBy("country")
    .agg(
        F.sum("age").alias("total_age"),
        F.count("*").alias("cnt"),
    )
    .withColumn("age_rank", F.dense_rank().over(w))
    .orderBy(F.col("cnt").desc())
)
