from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

scored = (
    prepared
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

filtered = (
    scored
    .crossJoin(scored.agg(F.avg("voltage").alias("overall_avg_voltage")))
    .filter(F.col("voltage") > F.col("overall_avg_voltage"))
    .drop("overall_avg_voltage")
)

screened = (
    filtered
    .crossJoin(filtered.agg(F.avg("kwh").alias("overall_avg_kwh")))
    .filter(F.col("kwh") > F.col("overall_avg_kwh"))
    .drop("overall_avg_kwh")
)

result = (
    screened
    .select(
        "id",
        "kwh",
        "voltage",
        "read_at_year",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
    .orderBy(F.col("voltage_filled").desc(), "id")
    .limit(10)
)
