from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 75000)
    .select("opened_date")
    .unionByName(df_accounts.filter(F.col("customer_id") == 3).select("opened_date"))
    .orderBy(F.col("opened_date").desc())
)
