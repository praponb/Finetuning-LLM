from pyspark.sql import functions as F

prepared = (
    df_tariff_windows
    .filter((F.col("tariff") == "FLAT") & (F.col("valid_to") < F.lit("2025-03-09 05:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
    )
)

tariff_side = (
    df_tariff_windows
    .select(
        F.lit("tariff").alias("src"),
        F.col("id").alias("ref_id"),
        F.col("valid_from").alias("event_time"),
        F.col("rate").alias("amount"),
    )
)

reading_side = (
    df_meter_readings
    .select(
        F.lit("reading").alias("src"),
        F.col("site_id").alias("ref_id"),
        F.col("read_at").alias("event_time"),
        F.col("voltage").alias("amount"),
    )
)

unioned = tariff_side.unionByName(reading_side)

result = (
    unioned
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ref_id").alias("total_ref_id"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("src")
)
