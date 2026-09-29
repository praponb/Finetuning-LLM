from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.countDistinct("country").alias("unique_countrys"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .filter(F.col("sbc.desktop_count") > 7)
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("c.name"),
        F.col("sbc.desktop_count"),
        F.col("sbc.unique_countrys"),
    )
)
