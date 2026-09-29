from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter(F.col("voltage") > 216.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_side = scoped.select("site_id", "id")

reading_rows = (
    df_meter_readings
    .filter((F.col("quality") != "EST") & (F.col("voltage") > 218.5))
    .select("id", "site_id")
)

all_events = reading_side.union(reading_rows)

result = (
    all_events
    .groupBy("site_id")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("site_id")
)
