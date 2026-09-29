from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .groupBy("status")
    .agg(F.max("opened_date").alias("latest_opened_date"))
    .orderBy("status")
)
