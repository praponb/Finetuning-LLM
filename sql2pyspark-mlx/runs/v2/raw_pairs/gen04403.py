from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .select(
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("c.country"),
        F.col("c.name"),
    )
)
