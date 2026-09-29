from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.customer_id").isNull())
    .select(
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("c.city"),
    )
    .orderBy("opened_date")
)
