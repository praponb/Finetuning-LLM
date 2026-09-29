from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.email").like("%@startup.io"))
    .groupBy(F.col("c.id"))
    .agg(
        F.count(F.col("a.id")).alias("account_count"),
        F.count(F.when(F.col("a.account_type") == "CHECKING", 1)).alias("checking_count"),
    )
)
