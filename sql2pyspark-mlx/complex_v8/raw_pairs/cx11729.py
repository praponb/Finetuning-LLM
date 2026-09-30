from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.hour(F.col("read_at")).alias("read_at_hour"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

with_details = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.zone").alias("site_zone"),
    )
)

tagged = (
    with_details
    .filter(F.col("voltage") > 222.6)
    .select("*", F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"))
)

result = (
    tagged
    .groupingSets(
        [["quality", "site_zone", "site_id"], ["quality"], []],
        "quality", "site_zone", "site_id",
    )
    .agg(
        F.grouping("quality").alias("is_total_quality"),
        F.max("read_at_hour").alias("last_read_at_hour"),
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("site_zone").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
