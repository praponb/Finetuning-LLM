from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(F.col("credit_limit") > 12600).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("a.opened_date") <= "2024-07-01")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.balance"),
    )
)
