from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.to_date("started_at") < "2023-06-07")
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("c.city"),
        F.col("C.device_list"),
        F.col("C.browser_list"),
    )
    .orderBy("city", F.col("segment").desc())
)
