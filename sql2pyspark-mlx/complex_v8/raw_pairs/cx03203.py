from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.col("quality").isin("OK", "EST"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

labeled = (
    prepared_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_group",
        F.coalesce(F.col("quality_group"), F.lit("UNKNOWN")).alias("quality_group_or_unknown"),
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

qualified = (
    labeled.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.hour(F.col("m.read_at")).between(10, 13)), "left_semi")
    .filter(F.col("voltage") > 220.2)
)

result = (
    qualified
    .rollup("quality_group_or_unknown", "quality", "quality_group")
    .agg(F.round(F.stddev("voltage"), 2).alias("stddev_voltage"))
    .select(
        F.coalesce(F.col("quality_group_or_unknown"), F.lit("ALL")).alias("quality_group_or_unknown"),
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        F.coalesce(F.col("quality_group"), F.lit("ALL")).alias("quality_group"),
        "stddev_voltage",
    )
    .orderBy(
        F.col("quality_group_or_unknown").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("quality_group").asc_nulls_last(),
    )
)
