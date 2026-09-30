from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count("balance").alias("balance_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.balance_count") > 5)
    .select(
        F.col("c.segment"),
        F.col("c.age"),
        F.col("t.status_values"),
        F.col("t.balance_count"),
    )
)
