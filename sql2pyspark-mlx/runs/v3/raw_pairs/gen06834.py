from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("method_upper", F.upper("method"))
    .withColumn("paid_at_day", F.col("paid_at").cast("date"))
)
