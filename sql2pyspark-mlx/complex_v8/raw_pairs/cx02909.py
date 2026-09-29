from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter((F.col("voltage") > 213.7) & (F.col("kwh") > 15.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
    )
)

with_activity = (
    recent.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.to_date(F.col("m.read_at")) < F.lit("2025-02-12").cast("date")) & (~F.col("m.quality").isin("EST", "OK")), "left_anti")
)

result = (
    with_activity
    .groupingSets(
        [["site_id", "quality"], ["site_id"], ["quality"]],
        "site_id", "quality",
    )
    .agg(F.array_sort(F.collect_set("quality")).alias("quality_list"))
)
