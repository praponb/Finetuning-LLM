from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .select(
        F.col("c.credit_limit"),
        F.col("c.email"),
        F.col("c.city"),
        F.col("a.balance"),
        F.col("a.customer_id"),
    )
)
