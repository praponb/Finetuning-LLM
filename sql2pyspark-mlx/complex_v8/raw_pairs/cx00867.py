from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

grouped = (
    base
    .groupBy(
        "quality",
        F.date_trunc("month", "read_at").alias("read_at_month"),
        "site_id",
    )
    .agg(
        F.round(F.stddev("voltage_bucket"), 2).alias("stddev_voltage_bucket"),
        F.sum(F.when(F.col("quality") == "BAD", 1).otherwise(0)).alias("n_quality_bad"),
        F.max("kwh_minus_voltage").alias("max_kwh_minus_voltage"),
        F.min("read_at").alias("first_read_at"),
        F.round(100 * F.count(F.when(F.col("quality").isin("OK", "EST"), 1)) / F.count("*"), 1).alias("pct_quality_in_ok_est"),
    )
)

joined_site = (
    grouped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    joined_site
    .groupBy("first_read_at")
    .pivot("quality", ["BAD", "EST"])
    .agg(F.sum("stddev_voltage_bucket"))
    .orderBy("first_read_at")
)
