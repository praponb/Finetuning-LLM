from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .filter(F.col("a.customer_id").isNotNull())
    .join(df_customers.filter(F.col("segment") == "SMB").alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("a.balance") <= 33000)
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.account_type"),
    )
    .orderBy(F.col("balance").desc(), F.col("account_type").desc(), "id")
    .limit(5)
)
