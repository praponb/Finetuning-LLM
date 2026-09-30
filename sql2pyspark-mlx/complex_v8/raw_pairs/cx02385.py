from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(7, 9)) & (F.col("voltage") >= 216.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

tagged = (
    prepared_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "greatest_kwh_voltage",
        F.abs(F.col("kwh") - 52.5).alias("kwh_distance"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown_2"),
    )
)

without_matches = (
    tagged.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") < 284.0) & (F.col("si.zone").isNotNull()), "left_semi")
)

result = (
    without_matches
    .groupBy("quality_or_unknown_2", "site_id")
    .pivot("quality_or_unknown", ["BAD", "EST", "UNKNOWN"])
    .agg(F.min("voltage").alias("min_v"), F.max("voltage").alias("max_v"))
)
