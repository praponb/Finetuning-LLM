from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([3, 35]))
    .select("opened_date", "balance", "customer_id")
)
