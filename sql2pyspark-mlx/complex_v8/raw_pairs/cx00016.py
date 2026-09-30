from pyspark.sql import functions as F

scoped_tariffs = (
    df_tariff_windows
    .filter((F.col("rate") <= 6.1) & (F.col("valid_from") < F.lit("2025-03-09 04:00:00").cast("timestamp")))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.coalesce(F.col("rate"), F.lit(0.0)).alias("rate_filled"),
    )
)

tariff_rows = (
    df_tariff_windows
    .filter((F.col("tariff") == "FLAT") & (F.month(F.col("valid_from")).isin(1, 3)))
    .select("id", "rate", "tariff")
)

tariff_side = df_tariff_windows.select("id", "rate", "tariff")

all_events = (
    tariff_rows
    .union(tariff_side)
    .distinct()
)

result = (
    all_events
    .groupBy("tariff")
    .agg(F.count("*").alias("n_rows"), F.sum("rate").alias("total_rate"))
    .orderBy("tariff")
)
