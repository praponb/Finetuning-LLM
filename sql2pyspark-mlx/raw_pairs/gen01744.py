from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count("balance").alias("balance_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.balance_count") > 8)
    .select(F.col("c.email"), F.col("cs.account_type_list"), F.col("cs.balance_count"))
    .orderBy(F.col("balance_count").desc())
)
