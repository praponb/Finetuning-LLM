from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 39)
    .select("opened_date", "account_type")
    .orderBy(F.col("opened_date").desc())
)
