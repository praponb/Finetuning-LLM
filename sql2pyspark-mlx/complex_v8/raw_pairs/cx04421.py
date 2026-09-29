from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter((F.col("voltage").between(221.4, 241.8)) & (F.month(F.col("read_at")).isin(2, 3)))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_side = (
    base_readings
    .filter((F.col("kwh") <= 81.3) & (F.col("site_id") >= 7))
    .select("site_id")
)

site_rows = (
    df_sites
    .filter((F.col("tariff") == "FLAT") & (F.col("installed_kw") <= 284.0))
    .select("id")
)

result = reading_side.subtract(site_rows)
