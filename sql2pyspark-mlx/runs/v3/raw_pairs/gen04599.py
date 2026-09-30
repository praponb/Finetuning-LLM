from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .groupBy("customer_id")
    .agg(
        F.max("balance").alias("max_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.stddev_balance") > 36)
    .select(
        F.col("c.age"),
        F.col("c.segment"),
        F.col("cs.max_balance"),
        F.col("cs.stddev_balance"),
    )
)
