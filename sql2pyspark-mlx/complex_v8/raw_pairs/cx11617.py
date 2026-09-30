from pyspark.sql import functions as F

valid = df_meter_readings.filter((F.col("voltage") < 234.5) | (F.month(F.col("read_at")).isin(2, 3)))

tagged = (
    valid
    .filter(F.col("voltage").between(213.7, 235.0))
    .select(
        "*",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.month(F.col("read_at")).alias("read_at_month"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

screened = (
    tagged.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") >= 42.1) & (F.col("m.read_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")), "left_anti")
)

filtered = (
    screened.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") < F.lit("2025-05-10").cast("date")), "left_semi")
)

result = (
    filtered
    .groupingSets(
        [["quality", "read_at_month", "site_id"], ["quality"], []],
        "quality", "read_at_month", "site_id",
    )
    .agg(
        F.percentile_approx("voltage_bucket", 0.5).alias("median_voltage_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "read_at_month",
        "site_id",
        "median_voltage_bucket",
        "distinct_ids",
    )
)
