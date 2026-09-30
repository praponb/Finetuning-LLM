from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
        F.when(F.col("voltage") >= 229.1, "A").when(F.col("voltage") >= 218.2, "B").otherwise("C").alias("voltage_tier"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

without_matches = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") == "Z4"), "left_anti")
)

result = (
    without_matches
    .groupBy("site_id", "voltage_tier")
    .pivot("quality", ["OK", "EST", "BAD"])
    .agg(F.sum("kwh").alias("amount"), F.max("kwh").alias("max_v"))
)
