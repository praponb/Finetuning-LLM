from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id").isNotNull() & (F.col("status") == "OPEN")) | ~F.col("status").isin(["CLOSED", "OPEN"]))
    .select("opened_date", "balance", "status")
)
