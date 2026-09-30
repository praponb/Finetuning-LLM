from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter(F.col("voltage") > 223.0)
    .select("id", "site_id", F.lit("reading").alias("src"))
)

reading_side = (
    df_meter_readings
    .filter((F.col("voltage") >= 224.8) & (F.col("read_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")))
    .select("site_id", "id", F.lit("reading_2").alias("src"))
)

stacked = reading_rows.union(reading_side)

result = (
    stacked
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"), F.sum("site_id").alias("total_site_id"))
    .orderBy("src")
)
