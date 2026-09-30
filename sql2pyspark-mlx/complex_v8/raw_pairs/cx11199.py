from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("voltage") > 222.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

reading_side = (
    base
    .filter(~F.col("quality").isin("BAD", "EST"))
    .select(F.col("id").alias("ref_id"), "quality")
)

tariff_rows = df_tariff_windows.select("id", "tariff")

result = reading_side.intersect(tariff_rows)
