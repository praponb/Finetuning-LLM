from pyspark.sql import functions as F

valid_readings = df_meter_readings.filter(F.col("site_id").isNotNull())

reading_rows = (
    valid_readings
    .select(
        F.col("quality").alias("name"),
        F.col("site_id").alias("entity_id"),
        F.lit("reading").alias("source"),
    )
)

site_rows = (
    df_sites
    .select(
        F.col("tariff").alias("name"),
        F.col("id").alias("entity_id"),
        F.lit("site").alias("source"),
    )
)

site_side = (
    df_sites
    .filter((F.length(F.trim(F.col("site_name"))) >= 4) & (F.year(F.col("commissioned_on")) == 2024))
    .select(
        F.col("zone").alias("name"),
        F.col("id").alias("entity_id"),
        F.lit("site").alias("source"),
    )
)

result = (
    reading_rows
    .unionByName(site_rows)
    .unionByName(site_side)
    .orderBy("source")
)
