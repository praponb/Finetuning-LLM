from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.id"), F.col("c.name"))
    .agg(
        F.count(F.col("a.id")).alias("account_count"),
        F.max(F.col("a.opened_date")).alias("last_opened_date"),
    )
    .filter(F.col("account_count") >= 2)
)
