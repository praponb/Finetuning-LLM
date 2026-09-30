from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(
        F.when(F.col("a.status").isin(["FROZEN", "OPEN"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg((F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"))
    .orderBy("status_group")
)
