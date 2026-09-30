from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .filter(F.col("balance") <= 50000)
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.name"),
        F.col("cs.account_type_values"),
        F.col("cs.avg_balance"),
    )
    .orderBy("id")
)
