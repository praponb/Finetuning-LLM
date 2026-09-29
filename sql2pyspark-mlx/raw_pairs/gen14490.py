from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.max("opened_date").alias("latest_opened_date"))
    .orderBy("customer_id")
    .limit(10)
)
