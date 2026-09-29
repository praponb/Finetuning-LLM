from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .select("id", "balance", "status", "customer_id")
)
