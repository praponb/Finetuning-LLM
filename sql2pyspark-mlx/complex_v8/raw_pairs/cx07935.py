from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter((F.col("quality") == "BAD") & (F.col("voltage") >= 216.1))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

without_matches = (
    valid_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

result = (
    without_matches
    .cube("site_id", "quality")
    .agg(
        F.round(100 * F.count(F.when(F.col("quality") == "OK", 1)) / F.count("*"), 1).alias("pct_quality_ok"),
        F.round(F.avg("kwh"), 2).alias("avg_kwh"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
)
