from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 39000)
    .select("id", "customer_id")
    .unionByName(
        df_accounts
        .filter(F.col("balance").between(66000, 72000))
        .select("id", "customer_id")
    )
    .orderBy("id")
)
