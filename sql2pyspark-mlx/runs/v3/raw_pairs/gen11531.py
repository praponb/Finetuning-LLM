from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([14, 24]) | F.col("balance").between(40000, 67000))
    .agg(F.max("opened_date").alias("last_opened_date"))
    .orderBy("last_opened_date")
)
