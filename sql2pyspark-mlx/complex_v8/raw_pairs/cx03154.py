from pyspark.sql import functions as F

base_tariffs = (
    df_tariff_windows
    .filter(F.col("rate").between(2.2, 7.6))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        (F.col("valid_from") + F.expr("INTERVAL 7 DAYS")).alias("valid_from_plus_7d"),
    )
)

tariff_side = (
    base_tariffs
    .filter(F.to_date(F.col("valid_from_plus_7d")) < F.lit("2025-02-13").cast("date"))
    .select(F.col("id").alias("entity_id"), F.col("tariff").alias("label"))
)

site_side = df_sites.select("id", "site_name")

reading_side = (
    df_meter_readings
    .filter((~F.col("quality").isin("BAD", "OK")) & (F.col("kwh") < 85.7))
    .select("site_id", "quality")
)

all_events = (
    tariff_side
    .union(site_side)
    .union(reading_side)
)

result = (
    all_events
    .groupBy("label")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
    )
    .orderBy("label")
)
