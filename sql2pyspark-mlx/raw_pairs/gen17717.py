from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(37000, 56000))
    .select("customer_id")
    .intersect(df_accounts.filter(F.col("customer_id") == 31).select("customer_id"))
)
