from pyspark.sql import functions as F

cleaned_tariffs = (
    df_tariff_windows
    .filter(F.col("rate") <= 5.7)
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.abs(F.col("rate") - 5.9).alias("rate_distance"),
    )
)

tariff_side = (
    cleaned_tariffs
    .filter((F.col("rate_distance").between(2.0, 4.8)) & (F.col("tariff") == "TOU"))
    .select(
        F.col("id").alias("ref_id"),
        F.col("valid_from").alias("event_time"),
        "tariff",
        F.lit("tariff").alias("src"),
    )
)

tariff_side_2 = (
    df_tariff_windows
    .filter(F.col("valid_from") < F.lit("2025-02-16 11:00:00").cast("timestamp"))
    .select(
        "id",
        "valid_from",
        "tariff",
        F.lit("tariff_2").alias("src"),
    )
)

all_events = tariff_side.union(tariff_side_2)

result = (
    all_events
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ref_id").alias("total_ref_id"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("src")
)
