from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter(F.col("kwh") < 109.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

reading_rows = (
    valid
    .filter((F.to_date(F.col("read_at")) < F.lit("2025-03-02").cast("date")) & (~F.col("quality").isin("OK", "EST")))
    .select(F.lit("reading").alias("src"), "quality", "quality_len")
)

tariff_side = (
    df_tariff_windows
    .filter(F.col("rate") <= 6.1)
    .select(
        F.lit("tariff").alias("src"),
        F.col("tariff").alias("quality"),
        F.col("id").alias("quality_len"),
    )
)

result = reading_rows.unionByName(tariff_side)
