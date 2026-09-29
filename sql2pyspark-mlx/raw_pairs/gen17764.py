from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 07:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.when(F.col("device_code").isin(["DV-02", "DV-01"]), "Group A").otherwise("Other").alias("device_code_group"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("pressure") >= 1029, "HIGH").otherwise("LOW").alias("pressure_tier"),
    )
    .agg(
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
    )
    .orderBy("span_minutes")
)
