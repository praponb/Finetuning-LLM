from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("a.balance") >= 35000) & F.col("a.status").isin(["CLOSED", "FROZEN"]))
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
        F.col("c.name"),
    )
    .orderBy("account_type")
)
