from pyspark.sql import functions as F

prepared = (
    df_sites
    .filter((F.year(F.col("commissioned_on")) == 2024) & (F.col("installed_kw") < 284.0))
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
        F.abs(F.col("installed_kw") - 96.9).alias("installed_kw_distance"),
    )
)

site_side = prepared.select(F.col("id").alias("qty"), F.col("installed_kw_distance").alias("value"))

reading_rows = df_meter_readings.select(F.col("id").alias("qty"), F.col("kwh").alias("value"))

result = site_side.union(reading_rows)
