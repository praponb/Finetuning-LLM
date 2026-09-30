from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .select(
        F.col("quality").alias("category"),
        F.col("read_at").alias("happened_at"),
        F.lit("reading").alias("src"),
    )
)

reading_rows_2 = df_meter_readings.select("quality", "read_at", F.lit("reading_2").alias("src"))

tariff_side = (
    df_tariff_windows
    .filter((F.col("rate") <= 5.5) & (F.col("tariff").isin("EV", "TOU")))
    .select("tariff", "valid_from", F.lit("tariff").alias("src"))
)

stacked = (
    reading_rows
    .union(reading_rows_2)
    .union(tariff_side)
)

result = (
    stacked
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("src")
)
