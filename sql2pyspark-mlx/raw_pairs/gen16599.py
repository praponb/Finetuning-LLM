from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(70000, 75000))
    .select("customer_id")
    .intersect(df_accounts.filter(F.col("customer_id") == 12).select("customer_id"))
)
