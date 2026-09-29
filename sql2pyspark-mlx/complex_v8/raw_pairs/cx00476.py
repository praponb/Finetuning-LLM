from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter((F.col("quality").isin("EST", "BAD")) & (F.col("kwh") > 33.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

reading_rows = filtered.select("quality", "voltage", F.lit("reading").alias("src"))

tariff_side = (
    df_tariff_windows
    .filter((F.col("rate") >= 2.2) & (F.col("tariff") == "FLAT"))
    .select("tariff", "rate", F.lit("tariff").alias("src"))
)

result = reading_rows.union(tariff_side)
