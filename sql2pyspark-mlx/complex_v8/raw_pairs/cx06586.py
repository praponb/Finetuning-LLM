from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter(F.month(F.col("read_at")).isin(2, 3))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

reading_side = scoped_readings.select("id", "read_at", F.col("quality").alias("category"))

tariff_rows = (
    df_tariff_windows
    .filter(F.col("valid_to") < F.lit("2025-03-27 13:00:00").cast("timestamp"))
    .select(
        "id",
        F.col("valid_to").alias("read_at"),
        F.col("tariff").alias("category"),
    )
)

all_events = reading_side.unionByName(tariff_rows)

result = (
    all_events
    .groupBy("category")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("read_at").alias("latest_read_at"),
    )
    .orderBy("category")
)
