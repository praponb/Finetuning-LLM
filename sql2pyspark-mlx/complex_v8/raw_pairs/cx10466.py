from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage") > 224.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

stats = (
    prepared_readings
    .filter(F.month(F.col("read_at")).isin(1, 3))
    .groupBy("site_id")
    .agg(
        F.round(F.sum("voltage") / F.nullif(F.sum("voltage_bucket"), F.lit(0)), 3).alias("voltage_to_voltage_bucket_ratio"),
        F.sum("voltage").alias("total_voltage"),
    )
)

with_details_site = (
    stats.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.site_name").isNotNull())
    .select("s.site_id", "s.voltage_to_voltage_bucket_ratio", "si.site_name")
)

result = (
    with_details_site
    .filter(F.lower(F.col("site_name")).like("%no%"))
    .select("site_id", "voltage_to_voltage_bucket_ratio", "site_name")
    .orderBy(
        F.col("site_id").desc_nulls_first(),
        F.col("site_name").desc_nulls_first(),
    )
)
