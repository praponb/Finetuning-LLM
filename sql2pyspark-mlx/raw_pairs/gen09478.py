from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"), "plan")
    .agg(F.min("end_date").alias("first_end_date"))
    .orderBy("first_end_date")
)
