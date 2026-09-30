from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(F.col("country").isin(["US", "TH", "DE"]))
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("browser")).alias("browser_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("c.credit_limit"),
        F.col("C.browser_list"),
    )
)
