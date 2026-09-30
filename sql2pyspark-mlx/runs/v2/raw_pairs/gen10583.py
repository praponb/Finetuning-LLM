from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.col("a.balance").between(33000, 79000) & F.col("a.opened_date").between("2024-09-01", "2025-08-01"))
    .select(
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("c.segment"),
        F.col("c.signup_date"),
    )
    .orderBy("segment")
)
