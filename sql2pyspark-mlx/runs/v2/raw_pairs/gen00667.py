from pyspark.sql import functions as F

result = (
    df_accounts
    .select("balance", "customer_id")
    .orderBy("balance", "customer_id")
)
