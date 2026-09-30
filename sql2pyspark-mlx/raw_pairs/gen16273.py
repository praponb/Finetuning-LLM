from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.sum(F.when(F.col("a.account_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
    .orderBy("credit_count")
)
