from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.credit_limit"),
        F.col("t.browser_list"),
        F.col("t.cnt"),
    )
    .orderBy("cnt")
)
