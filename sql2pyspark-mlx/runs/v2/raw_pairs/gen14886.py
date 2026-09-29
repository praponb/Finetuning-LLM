from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.balance") > 37000)
    .groupBy(F.col("c.country"))
    .agg(
        F.count(F.col("a.id")).alias("account_count"),
        F.count(F.when(F.col("a.account_type") == "CREDIT", 1)).alias("credit_count"),
    )
    .filter(F.col("credit_count") >= 2)
)
