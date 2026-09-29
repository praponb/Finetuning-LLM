from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2024-11-07")
    .groupBy("customer_id")
    .pivot("status", ["CLOSED", "FROZEN"])
    .agg(F.avg("balance"))
    .orderBy("customer_id")
)
