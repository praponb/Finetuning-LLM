from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 200) | (F.col("end_date") <= "2024-02-01"))
    .withColumn("status_upper", F.upper("status"))
    .withColumn("monthly_fee_filled", F.coalesce("monthly_fee", F.lit(0)))
)
