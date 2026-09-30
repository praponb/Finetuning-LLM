from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([11, 17, 26]))
    .groupBy(F.month("opened_date").alias("opened_month"), "account_type")
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"),
        F.max("opened_date").alias("last_opened_date"),
    )
    .filter(F.col("open_count") < 29)
)
