from pyspark.sql import functions as F

enriched = (
    df_sessions
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(F.length(F.col("p.name")) >= 8)
    .select("s.id", "s.customer_id", "s.device", F.col("p.email"))
)

result = (
    enriched
    .groupBy("device")
    .agg(
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
    )
    .orderBy(F.col("device").asc_nulls_last())
)
