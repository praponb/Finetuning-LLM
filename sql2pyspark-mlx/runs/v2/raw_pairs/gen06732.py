from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([8, 14, 22, 24]))
    .select("account_type", "status")
    .unionByName(
        df_accounts
        .filter(F.col("customer_id").isin([24, 38, 39]))
        .select("account_type", "status")
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
