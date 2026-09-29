from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .select(
        F.lit("reading").alias("record_type"),
        "quality",
        F.col("id").alias("entity_id"),
    )
)

tariff_side = df_tariff_windows.select(F.lit("tariff").alias("record_type"), "tariff", "id")

all_events = reading_side.union(tariff_side)

result = (
    all_events
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
    )
    .orderBy("record_type")
)
