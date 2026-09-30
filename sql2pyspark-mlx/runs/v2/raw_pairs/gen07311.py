from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.email"),
        F.col("c.name"),
        F.col("a.status"),
        F.col("a.customer_id"),
    )
    .orderBy("status")
)
