from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([22, 35]) & F.col("status").isin(["CLOSED", "FROZEN"]))
    .groupBy("account_type")
    .agg(F.min("opened_date").alias("min_opened_date"))
)
