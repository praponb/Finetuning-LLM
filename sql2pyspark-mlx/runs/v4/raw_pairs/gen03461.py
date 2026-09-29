from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("num_sessions"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.num_sessions") >= 5)
    .select(
        F.col("c.city"),
        F.col("c.name"),
        F.col("t.num_sessions"),
        F.col("t.browser_list"),
    )
    .orderBy(F.col("city").desc())
)
