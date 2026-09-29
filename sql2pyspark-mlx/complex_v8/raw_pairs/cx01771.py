from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
    )
)

reading_side = (
    df_meter_readings
    .select(
        F.lit("reading").alias("src"),
        "id",
        F.col("site_id").alias("key_id"),
        F.col("read_at").alias("happened_at"),
    )
)

reading_rows = (
    df_meter_readings
    .select(
        F.lit("reading_2").alias("src"),
        F.col("site_id").alias("id"),
        F.col("id").alias("key_id"),
        F.col("read_at").alias("happened_at"),
    )
)

unioned = reading_side.unionByName(reading_rows)

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
