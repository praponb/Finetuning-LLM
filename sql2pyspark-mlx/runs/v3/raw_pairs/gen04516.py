from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.month("opened_date").alias("opened_month"), "status")
    .agg(F.max("opened_date").alias("last_opened_date"))
    .orderBy("last_opened_date")
)
