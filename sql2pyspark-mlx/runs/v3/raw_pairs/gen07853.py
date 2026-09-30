from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.closed_count") > 76)
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("c.country"),
        F.col("cs.account_type_values"),
        F.col("cs.closed_count"),
    )
    .orderBy("segment")
)
