from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("c.segment") != "SMB")
    .select(
        F.col("c.credit_limit"),
        F.col("c.is_active"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.length(F.col("a.account_type")).alias("account_type_length"),
    )
)
