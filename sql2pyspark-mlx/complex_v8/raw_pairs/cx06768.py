from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .select(
        "site_id",
        "read_at",
        F.col("voltage").alias("value"),
        F.lit("reading").alias("src"),
    )
)

reading_side = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage") < 234.5))
    .select(
        "id",
        "read_at",
        "kwh",
        F.lit("reading_2").alias("src"),
    )
)

stacked = reading_rows.union(reading_side)

result = (
    stacked
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("value").alias("total_value"),
        F.max("read_at").alias("latest_read_at"),
    )
    .orderBy("src")
)
