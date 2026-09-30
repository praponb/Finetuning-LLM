from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.email").isNotNull())
    .groupBy(
        F.year(F.col("c.signup_date")).alias("signup_year"),
        F.when(F.col("c.country").isin(["DE", "JP"]), "group_a").otherwise("other").alias("country_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.device")))).alias("device_values"),
        F.countDistinct(F.col("s.device")).alias("unique_devices"),
    )
    .orderBy("device_values")
)
