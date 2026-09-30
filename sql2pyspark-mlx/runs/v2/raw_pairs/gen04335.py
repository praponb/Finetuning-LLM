from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 46000)
    .select("status", "customer_id")
    .unionByName(
        df_accounts
        .filter(F.col("customer_id").isin([14, 15, 16, 26]))
        .select("status", "customer_id")
    )
)
