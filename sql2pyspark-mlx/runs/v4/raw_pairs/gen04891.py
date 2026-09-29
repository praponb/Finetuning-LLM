from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") < "2023-05-17") & ((F.col("customer_id") == 32) | (F.year("start_date") == 2025)))
    .withColumn("plan_upper", F.upper("plan"))
    .withColumn("start_date_plus_31d", F.date_add("start_date", 31))
)
