from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.col("a.balance") > 55000) & F.col("a.customer_id").isin([12, 28]))
    .select(
        F.col("c.city"),
        F.col("c.signup_date"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("a.balance"),
    )
)
