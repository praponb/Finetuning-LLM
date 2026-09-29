from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.col("customer_id").isin([13, 30])).alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_semi")
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.segment"),
        F.col("c.is_active"),
    )
)
