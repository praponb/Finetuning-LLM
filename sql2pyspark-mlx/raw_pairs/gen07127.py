from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc())
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy("device_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("started_at") > F.to_timestamp(F.lit("2023-09-15 16:00:00")))
    .select("*", F.length("device").alias("device_length"))
)

latest = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "started_at", "device_length")
)

scored = (
    latest
    .select(
        "*",
        F.avg("device_length").over(w2).alias("customer_id_avg_device_length"),
        F.last("device_length", ignorenulls=True).over(w3).alias("last_known_device_length"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

result = (
    kept
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("customer_id_avg_device_length", 0.5).alias("customer_id_avg_device_length_p50"),
        F.round(F.avg("last_known_device_length"), 2).alias("avg_last_known_device_length"),
        F.min("started_at").alias("first_started_at"),
    )
    .filter(F.col("avg_last_known_device_length") >= 0)
)
