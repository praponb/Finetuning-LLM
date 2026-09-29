from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.status") == "FROZEN")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("c.name"),
    )
    .orderBy("status", "id")
    .limit(3)
)
