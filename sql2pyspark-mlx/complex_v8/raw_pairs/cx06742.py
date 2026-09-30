from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter((F.col("kwh") < 82.9) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

screened = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.lower(F.col("si.site_name")).like("%de%")), "left_semi")
)

result = (
    screened
    .groupBy("read_at")
    .pivot("quality", ["OK", "BAD"])
    .agg(F.count("kwh"))
)
