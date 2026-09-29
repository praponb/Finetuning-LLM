from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("c.segment").isin(["Corporate", "Consumer"]))
    .select(
        F.col("c.segment"),
        F.col("c.email"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
    )
)
