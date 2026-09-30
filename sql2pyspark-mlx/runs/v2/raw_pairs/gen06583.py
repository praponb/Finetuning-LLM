from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(
        F.max(F.col("a.opened_date")).alias("last_opened_date"),
        F.min(F.col("a.balance")).alias("min_balance"),
    )
)
