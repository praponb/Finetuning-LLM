from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(~F.col("country").isin(["TH", "US", "DE"]))
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.max("started_at").alias("last_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("ct.country_list"),
        F.col("ct.last_started_at"),
    )
    .orderBy("last_started_at", "id")
    .limit(5)
)
