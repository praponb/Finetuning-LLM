from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("num_accounts"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.num_accounts") >= 4)
    .select(F.col("c.is_active"), F.col("cs.status_list"), F.col("cs.num_accounts"))
    .orderBy(F.col("num_accounts").desc())
)
