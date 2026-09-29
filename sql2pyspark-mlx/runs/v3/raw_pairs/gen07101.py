from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select("id", "status", F.add_months("end_date", 3).alias("end_date_plus_months"))
    .orderBy("status", F.col("end_date_plus_months").desc(), "id")
    .limit(10)
)
