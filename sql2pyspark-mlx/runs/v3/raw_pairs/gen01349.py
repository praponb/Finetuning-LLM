from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.city") == "Chiang Mai")
    .groupBy(F.col("c.city"))
    .agg(
        (F.max(F.col("a.balance")) - F.min(F.col("a.balance"))).alias("balance_range"),
        F.count(F.col("a.balance")).alias("balance_count"),
    )
)
