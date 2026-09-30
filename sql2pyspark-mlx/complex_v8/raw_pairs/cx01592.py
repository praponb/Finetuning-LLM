from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("voltage") < 241.8)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 0.9, 2).alias("kwh_adj"),
    )
)

reading_rows = df_meter_readings.select("quality", F.col("read_at").alias("happened_at"))

reading_rows_2 = df_meter_readings.select("quality", F.col("read_at").alias("happened_at"))

result = (
    reading_rows
    .union(reading_rows_2)
    .orderBy("quality")
)
