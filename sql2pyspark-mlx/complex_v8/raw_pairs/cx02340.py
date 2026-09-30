from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter(F.col("kwh") <= 113.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
    )
)

qualified = (
    recent.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") > 223.1), "left_semi")
)

labeled = (
    qualified
    .withColumn("least_kwh_voltage_distance", F.abs(F.col("least_kwh_voltage") - 171.5))
)

scored = (
    labeled
    .select(
        "*",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.least(F.col("least_kwh_voltage"), F.col("kwh")).alias("least_least_kwh_voltage_kwh"),
    )
)

rollup_base = (
    scored
    .groupBy(
        F.date_trunc("month", "read_at").alias("read_at_month"),
        "quality",
        "site_id",
    )
    .agg(
        F.round(F.stddev("kwh_filled"), 2).alias("stddev_kwh_filled"),
        F.count("*").alias("row_count"),
        F.max("read_at").alias("last_read_at"),
        F.round(F.sum("least_kwh_voltage"), 2).alias("total_least_kwh_voltage"),
    )
    .filter(F.col("row_count") >= 2)
)

annotated = (
    rollup_base
    .filter(F.col("row_count") < 10)
    .select(
        "read_at_month",
        "quality",
        "site_id",
        "stddev_kwh_filled",
        "last_read_at",
        (F.floor(F.col("total_least_kwh_voltage") / 250) * 250).alias("total_least_kwh_voltage_bucket"),
    )
)

enriched = (
    annotated
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("last_read_at")) - F.unix_timestamp(F.col("read_at_month"))) / 3600, 1).alias("hours_read_at_month_to_last_read_at"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

result = (
    enriched
    .select(
        "read_at_month",
        "quality",
        "stddev_kwh_filled",
        "total_least_kwh_voltage_bucket",
        "quality_prefix",
    )
    .orderBy(F.col("read_at_month").desc())
)
