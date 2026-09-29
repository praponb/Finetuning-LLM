from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.name"),
        F.col("C.status_list"),
        F.col("C.avg_balance"),
    )
    .orderBy(F.col("avg_balance").desc(), "id")
    .limit(1)
)
