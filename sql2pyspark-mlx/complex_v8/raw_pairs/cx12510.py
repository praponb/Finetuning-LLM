from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 52.4)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
    )
)

tagged = (
    valid_readings
    .filter(F.col("kwh") >= 45.0)
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        (F.floor(F.col("greatest_voltage_kwh") / 50) * 50).alias("greatest_voltage_kwh_bucket"),
    )
)

screened = (
    tagged.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") >= 141.6), "left_semi")
)

with_details = (
    screened.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select("s.*", F.col("si.zone").alias("site_zone"))
)

result = (
    with_details
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_len",
        "greatest_voltage_kwh",
        "days_since_read_at",
        F.greatest(F.col("voltage_filled"), F.col("days_since_read_at")).alias("greatest_voltage_filled_days_since_read_at"),
    )
    .orderBy(F.col("days_since_read_at").asc_nulls_last())
)
