from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2024-11-01")
    .select("customer_id")
    .unionByName(df_accounts.filter(F.col("status").isNull()).select("customer_id"))
    .orderBy(F.col("customer_id").desc())
)
