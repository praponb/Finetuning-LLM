from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("city")

result = (
    df_customers
    .filter(F.col("credit_limit").between(5100, 13700))
    .groupBy("city")
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .withColumn("cumulative_credit_limit", F.sum("total_credit_limit").over(w))
    .orderBy(F.col("cumulative_credit_limit").desc())
)
