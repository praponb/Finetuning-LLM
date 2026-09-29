from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .select(
        "site_id",
        F.col("read_at").alias("event_time"),
        F.lit("reading").alias("record_type"),
    )
)

reading_side_2 = (
    df_meter_readings
    .select("site_id", "read_at", F.lit("reading_2").alias("record_type"))
)

reading_rows = df_meter_readings.select("site_id", "read_at", F.lit("reading_3").alias("record_type"))

combined = (
    reading_side
    .union(reading_side_2)
    .union(reading_rows)
)

result = (
    combined
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("site_id").alias("total_site_id"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("record_type")
)
