from pyspark.sql import functions as F

joined = (
    df_sessions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.name"))
)

result = (
    joined
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.max("started_at").alias("last_seen"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
    )
    .orderBy(F.col("sg_events").asc_nulls_last())
)
