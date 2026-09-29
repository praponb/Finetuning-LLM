from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(69000, 71000) & (F.year("opened_date") >= 2023) & F.col("balance").between(28000, 75000))
    .select("customer_id", "opened_date", "account_type")
    .orderBy(F.col("customer_id").desc())
)
