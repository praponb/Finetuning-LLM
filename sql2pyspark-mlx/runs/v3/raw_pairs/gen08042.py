from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("A"), F.col("c.id") == F.col("A.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("A.pct_open"),
        F.col("A.status_values"),
    )
    .orderBy(F.col("age").desc(), "pct_open")
)
