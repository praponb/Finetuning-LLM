from pyspark.sql import functions as F

result = (
    df_accounts
    .select("customer_id", "status", "balance")
    .orderBy("status")
)
