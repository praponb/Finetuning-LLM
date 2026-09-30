from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter((F.col("quality").isNotNull()) & (F.col("kwh") >= 51.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

reading_side = (
    scoped_readings
    .filter(~F.col("quality").isin("EST", "BAD"))
    .select("id", "site_id")
)

reading_rows = (
    df_meter_readings
    .filter(F.col("voltage") > 216.1)
    .select("id", "site_id")
)

result = reading_side.unionByName(reading_rows)
