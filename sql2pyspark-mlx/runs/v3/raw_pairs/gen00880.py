from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(~F.col("c.is_active"))
    .select(
        F.col("s.device"),
        F.col("c.city"),
        F.col("c.is_active"),
        F.substring(F.col("c.city"), 1, 4).alias("city_prefix"),
    )
    .orderBy("device", "city_prefix")
)
