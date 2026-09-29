from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("balance") >= 23000)
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.pct_open") > 53)
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.age"),
        F.col("t.pct_open"),
        F.col("t.savings_balance"),
    )
)
