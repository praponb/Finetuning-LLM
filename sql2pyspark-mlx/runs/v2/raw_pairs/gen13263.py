from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 30000)
    .select("customer_id", "opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("status") == "FROZEN")
        .select("customer_id", "opened_date")
    )
    .distinct()
    .orderBy("opened_date")
)
