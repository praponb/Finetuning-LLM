from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter(F.col("zone") != "Z3")
    .select(
        "id",
        F.col("site_name").alias("name"),
        "installed_kw",
        F.lit("site").alias("origin"),
    )
)

reading_rows = (
    df_meter_readings
    .filter((F.col("voltage") <= 229.1) & (F.col("quality") == "OK"))
    .select(
        "id",
        "quality",
        "voltage",
        F.lit("reading").alias("origin"),
    )
)

result = (
    site_rows
    .union(reading_rows)
    .orderBy(F.col("id").desc())
)
