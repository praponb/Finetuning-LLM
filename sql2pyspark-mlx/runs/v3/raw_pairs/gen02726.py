from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.chrome_count") > 9)
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("cs.country_list"),
        F.col("cs.chrome_count"),
    )
    .orderBy(F.col("id").desc())
)
