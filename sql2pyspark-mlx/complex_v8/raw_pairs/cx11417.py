from pyspark.sql import functions as F

filtered_tariffs = (
    df_tariff_windows
    .filter(F.hour(F.col("valid_from")).between(10, 13))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
    )
)

tariff_side = filtered_tariffs.select("id", F.col("tariff").alias("category"))

reading_rows = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")) & (F.col("voltage").between(213.2, 233.1)))
    .select(F.col("site_id").alias("id"), F.col("quality").alias("category"))
)

combined = tariff_side.unionByName(reading_rows)

result = (
    combined
    .groupBy("category")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("category")
)
