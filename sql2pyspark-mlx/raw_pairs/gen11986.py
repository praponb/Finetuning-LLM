from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.segment").isin(["SMB", "Corporate", "Consumer"]))
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("c.email"),
    )
    .orderBy(F.col("balance").desc())
)
