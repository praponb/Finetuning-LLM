from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .select("id", "opened_date", "customer_id", "status")
)
