from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-09-19 02:00:00").cast("timestamp")) & (F.col("event_time") >= "2023-08-01"))
    .withColumn("event_type_upper", F.upper("event_type"))
    .withColumn("duration_sec_whole", F.floor("duration_sec"))
)

result = (
    base
    .withColumn("week_start", F.date_trunc("week", "event_time"))
    .rollup("week_start")
    .agg(
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("duration_sec_p95").desc_nulls_last())
)
