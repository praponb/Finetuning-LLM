from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.signup_date"),
        F.col("c.is_active"),
        F.col("c.city"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.lower(F.col("c.segment")).alias("segment_lower"),
    )
)
