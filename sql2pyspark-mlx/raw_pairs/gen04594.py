from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_year")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 170)
    .groupBy(F.year("start_date").alias("start_year"))
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("cumulative_monthly_fee", F.sum("total_monthly_fee").over(w))
)
