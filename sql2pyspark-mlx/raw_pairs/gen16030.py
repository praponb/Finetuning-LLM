from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.customer_id").isin([2, 21, 26, 36]))
    .groupBy(F.col("c.segment"), F.col("c.city"))
    .agg(
        F.count(F.when(F.col("a.account_type") == "CREDIT", 1)).alias("credit_count"),
        F.count(F.col("a.balance")).alias("balance_count"),
    )
    .orderBy(F.col("credit_count").desc())
)
