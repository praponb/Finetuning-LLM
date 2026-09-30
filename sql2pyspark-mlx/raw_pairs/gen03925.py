from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.age") >= 57) & (F.col("a.balance") >= 50000))
    .select(
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.email"),
    )
)
