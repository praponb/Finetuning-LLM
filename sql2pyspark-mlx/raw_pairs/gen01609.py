from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("opened_date") < "2024-10-05")
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.balance_range") > 27)
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("cs.stddev_balance"),
        F.col("cs.balance_range"),
    )
    .orderBy(F.col("stddev_balance").desc(), "id")
    .limit(20)
)
