from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select("id", "customer_id", "account_type")
    .subtract(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("id", "customer_id", "account_type")
    )
    .orderBy("customer_id")
)
