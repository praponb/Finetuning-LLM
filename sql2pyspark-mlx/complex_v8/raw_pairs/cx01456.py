from pyspark.sql import functions as F

reading_side = df_meter_readings.select("id", F.col("read_at").alias("event_time"), "quality")

reading_side_2 = (
    df_meter_readings
    .filter(F.col("voltage") <= 237.5)
    .select("id", "read_at", "quality")
)

stacked = reading_side.union(reading_side_2)

result = (
    stacked
    .groupBy("quality")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("quality")
)
