from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_month")

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.sum("age").alias("total_age"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_age", F.sum("total_age").over(w))
)
