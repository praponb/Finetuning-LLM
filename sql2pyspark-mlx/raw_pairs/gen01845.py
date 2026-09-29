from pyspark.sql import functions as F

result = (
    df_accounts
    .select("id", "opened_date", "customer_id", "balance")
    .orderBy(F.col("id").desc())
)
