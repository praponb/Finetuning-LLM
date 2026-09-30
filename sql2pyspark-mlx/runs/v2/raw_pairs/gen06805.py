from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([12, 24]) & (F.col("end_date") < "2023-06-13"))
    .withColumn("plan_group", F.when(F.col("plan").isin(["premium", "standard"]), "Tier 1").otherwise("Other"))
)
