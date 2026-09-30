from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .pivot("status", ["OPEN", "CLOSED"])
    .agg(F.sum("balance"))
    .orderBy("customer_id")
)
