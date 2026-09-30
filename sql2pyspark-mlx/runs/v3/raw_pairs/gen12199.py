from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.customer_id").isin([9, 24, 34]))
    .select(
        F.col("a.balance"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("c.segment"),
        F.col("c.country"),
    )
)
