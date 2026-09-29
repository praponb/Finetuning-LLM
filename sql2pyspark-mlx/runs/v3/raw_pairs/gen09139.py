from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("c.country").isin(["JP", "US"]) & F.col("c.country").isin(["JP", "US"]))
    .select(
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("c.name"),
        F.col("c.age"),
    )
)
