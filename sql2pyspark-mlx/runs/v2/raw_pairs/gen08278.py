from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.sum("balance").alias("total_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.name"),
        F.col("t.stddev_balance"),
        F.col("t.total_balance"),
    )
    .orderBy(F.col("id").desc(), F.col("stddev_balance").desc())
)
