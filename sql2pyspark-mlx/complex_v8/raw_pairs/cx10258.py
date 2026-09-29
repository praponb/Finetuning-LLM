from pyspark.sql import functions as F

tariff_rows = (
    df_tariff_windows
    .filter(F.col("valid_to") >= F.lit("2025-01-25 21:00:00").cast("timestamp"))
    .select(
        F.lit("tariff").alias("src"),
        "id",
        F.col("valid_to").alias("happened_at"),
    )
)

reading_side = (
    df_meter_readings
    .select(
        F.lit("reading").alias("src"),
        "id",
        F.col("read_at").alias("happened_at"),
    )
)

unioned = tariff_rows.unionByName(reading_side)

result = (
    unioned
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("src")
)
