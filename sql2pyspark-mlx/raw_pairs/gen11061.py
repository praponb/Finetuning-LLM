from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(47000, 74000))
    .orderBy("customer_id", "id")
)
