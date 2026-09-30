from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_monthly_fee").desc())

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-09-01", "2024-03-30"))
    .groupBy("customer_id")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("rnk").desc())
)
