from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.to_date("paid_at") >= "2024-07-16")
    .withColumn("method_upper", F.upper("method"))
)

agg = (
    valid_payments
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = agg.orderBy("stddev_amount")
