from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.account_type") == "SAVINGS")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("c.country"),
        F.col("c.credit_limit"),
        F.col("c.age"),
    )
)
