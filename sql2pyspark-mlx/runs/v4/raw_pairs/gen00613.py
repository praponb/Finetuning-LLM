from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_age").desc())

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.sum("age").alias("total_age"),
        F.count("*").alias("cnt"),
    )
    .withColumn("age_rank", F.rank().over(w))
    .orderBy(F.col("cnt").desc(), "signup_month")
    .limit(20)
)
