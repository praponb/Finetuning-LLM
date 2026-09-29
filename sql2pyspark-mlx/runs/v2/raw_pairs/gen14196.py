from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date").between("2023-09-04", "2024-10-01") & F.col("status").isin(["CLOSED", "FROZEN", "OPEN"])) | (F.col("status") == "CLOSED"))
    .select("customer_id", "status", F.round("balance", 0).alias("balance_rounded"))
)
