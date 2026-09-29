from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("customer_id", F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.max("started_at").alias("ended_at"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
    )
)
