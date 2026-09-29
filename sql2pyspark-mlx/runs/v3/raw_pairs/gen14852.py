from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([2, 10, 22]))
    .select("status", "customer_id")
    .unionByName(
        df_accounts
        .filter(F.col("customer_id").isNotNull())
        .select("status", "customer_id")
    )
)
