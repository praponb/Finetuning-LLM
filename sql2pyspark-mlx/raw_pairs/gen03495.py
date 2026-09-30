from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .select(
        F.col("c.segment"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("a.opened_date"),
    )
)
