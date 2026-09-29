from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter((F.col("quality") == "BAD") & (F.col("voltage") >= 218.2))
    .select(F.col("quality").alias("category"), F.col("id").alias("qty"), "read_at")
)

tariff_rows = (
    df_tariff_windows
    .select(
        F.col("tariff").alias("category"),
        F.col("id").alias("qty"),
        F.col("valid_from").alias("read_at"),
    )
)

result = (
    reading_rows
    .unionByName(tariff_rows)
    .distinct()
)
