from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("abc"), F.col("c.id") == F.col("abc.customer_id"), "left")
    .select(
        F.col("c.city"),
        F.col("c.segment"),
        F.col("abc.stddev_balance"),
        F.col("abc.status_values"),
    )
)
