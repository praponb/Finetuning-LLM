from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter(F.col("voltage") >= 215.1)
    .select("id", "site_id")
)

reading_rows_2 = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-03-08 10:00:00").cast("timestamp"))
    .select("id", "site_id")
)

result = reading_rows.union(reading_rows_2)
