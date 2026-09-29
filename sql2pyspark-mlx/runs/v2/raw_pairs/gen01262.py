from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(~((F.col("a.account_type") == "SAVINGS") & F.col("a.opened_date").between("2023-10-01", "2025-10-01")))
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("c.name"),
        F.col("c.country"),
        F.col("c.credit_limit"),
    )
)
