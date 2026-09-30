from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .filter(F.col("ct.stddev_balance") > 41)
    .select(F.col("c.signup_date"), F.col("ct.stddev_balance"), F.col("ct.avg_balance"))
    .orderBy("avg_balance")
)
