from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(7, 9))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_side = valid.select("site_id")

site_side = (
    df_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("D%")) & (F.col("commissioned_on") >= F.lit("2024-09-15").cast("date")))
    .select("id")
)

result = (
    reading_side
    .subtract(site_side)
    .orderBy(F.col("site_id").desc())
)
