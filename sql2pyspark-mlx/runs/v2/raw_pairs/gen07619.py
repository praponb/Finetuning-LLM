from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("browser") == "Chrome")
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.sum(F.when(F.col("device") == "desktop", 1).otherwise(0)).alias("desktop_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.age"),
        F.col("t.device_list"),
        F.col("t.desktop_count"),
    )
)
