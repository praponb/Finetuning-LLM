from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 21)
    .withColumn("status_upper", F.upper("status"))
    .withColumn("plan_upper", F.upper("plan"))
)
