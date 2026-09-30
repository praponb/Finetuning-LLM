from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(~F.col("c.segment").isin(["SMB", "Corporate", "Consumer"]) | F.col("a.balance").between(37000, 73000))
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("c.name"),
    )
)
