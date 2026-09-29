from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_amount") * 100) / F.sum("total_amount").over(w)), 2))
    .orderBy("paid_month", F.col("pct_of_total").desc())
)
