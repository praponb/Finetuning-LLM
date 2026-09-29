from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN"]) & F.col("customer_id").isin([7, 9]) & (F.col("status") == "OPEN"))
    .select("opened_date")
)
