from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter((F.col("kwh") > 50.4) & (F.col("quality") == "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh_2"),
    )
)

filtered_2 = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
    .filter(F.col("site_id").isNotNull())
)

enriched = (
    filtered_2
    .filter(F.col("least_kwh_voltage") >= 98.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.when(F.col("least_kwh_voltage") >= 208.2, "HIGH").when(F.col("least_kwh_voltage") >= 49.0, "MEDIUM").otherwise("LOW").alias("least_kwh_voltage_tier"),
        F.coalesce(F.col("least_kwh_voltage"), F.lit(0.0)).alias("least_kwh_voltage_filled"),
        F.coalesce(F.col("voltage_per_kwh_2"), F.lit(0.0)).alias("voltage_per_kwh_2_filled"),
    )
)

result = (
    enriched
    .groupingSets(
        [["site_id", "least_kwh_voltage_tier"], ["site_id"]],
        "site_id", "least_kwh_voltage_tier",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("least_kwh_voltage_filled") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("least_kwh_voltage_filled_to_voltage_ratio"),
    )
    .select(
        "site_id",
        F.coalesce(F.col("least_kwh_voltage_tier"), F.lit("ALL")).alias("least_kwh_voltage_tier"),
        "grouping_level",
        "least_kwh_voltage_filled_to_voltage_ratio",
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("least_kwh_voltage_tier").asc_nulls_last(),
    )
)
