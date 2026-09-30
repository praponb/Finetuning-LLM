from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter(F.col("quality").isin("OK", "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

reading_side = (
    cleaned_readings
    .filter((F.col("kwh").between(9.5, 87.1)) & (F.col("site_id") >= 7))
    .select(F.col("quality").alias("label"), "site_id")
)

tariff_side = (
    df_tariff_windows
    .filter((F.col("rate") <= 6.1) & (F.col("tariff").isNotNull()))
    .select(F.col("tariff").alias("label"), F.col("id").alias("site_id"))
)

unioned = reading_side.union(tariff_side)

result = (
    unioned
    .groupBy("label")
    .agg(F.count("*").alias("n_rows"), F.sum("site_id").alias("total_site_id"))
    .orderBy("label")
)
