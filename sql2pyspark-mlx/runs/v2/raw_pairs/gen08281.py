from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_year")

result = (
    df_customers
    .filter(F.col("city") != "London")
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .withColumn("cumulative_credit_limit", F.sum("total_credit_limit").over(w))
    .orderBy("total_credit_limit")
)
