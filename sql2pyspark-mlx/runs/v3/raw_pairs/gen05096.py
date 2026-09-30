from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.segment"),
        F.col("c.credit_limit"),
        F.col("c.is_active"),
        F.col("a.status"),
    )
    .orderBy("segment", F.col("status").desc())
)
