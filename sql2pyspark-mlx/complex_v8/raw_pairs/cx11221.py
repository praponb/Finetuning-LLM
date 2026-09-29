from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .select(
        F.col("id").alias("key_id"),
        F.col("read_at").alias("event_time"),
        "voltage",
    )
)

tariff_side = (
    df_tariff_windows
    .filter((F.col("tariff") == "TOU") & (F.month(F.col("valid_from")).isin(2, 3)))
    .select(
        F.col("id").alias("key_id"),
        F.col("valid_to").alias("event_time"),
        F.col("rate").alias("voltage"),
    )
)

combined = reading_rows.unionByName(tariff_side)

result = (
    combined
    .groupBy("key_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("voltage").alias("total_voltage"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("key_id")
)
