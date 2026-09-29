from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "CLOSED") & F.col("opened_date").between("2025-08-01", "2025-10-01"))
    .select("status", F.col("balance").cast("int").alias("balance_int"))
)
