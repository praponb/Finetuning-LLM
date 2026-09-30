from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.upper(F.trim(F.col("c.email"))) == "HENRY@STARTUP.IO")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.segment"),
    )
)
