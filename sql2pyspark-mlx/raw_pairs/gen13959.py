from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_payments
    .filter(F.col("status") != "SUCCESS")
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("pct_of_total", F.round(((F.col("total_amount") * 100) / F.sum("total_amount").over(w)), 2))
)
