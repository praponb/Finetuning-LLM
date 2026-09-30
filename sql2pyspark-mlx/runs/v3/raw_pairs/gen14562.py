from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") > 15600)
    .groupBy(F.col("c.segment"))
    .agg((F.max(F.col("a.balance")) - F.min(F.col("a.balance"))).alias("balance_range"))
    .orderBy(F.col("balance_range").desc())
)
