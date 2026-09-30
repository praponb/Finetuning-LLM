from pyspark.sql import functions as F

cleaned_tariffs = (
    df_tariff_windows
    .filter((F.col("rate") <= 5.9) & (F.col("valid_to") < F.lit("2025-03-09 04:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.abs(F.col("rate") - 5.5).alias("rate_distance"),
    )
)

tariff_rows = (
    cleaned_tariffs
    .filter((F.col("tariff").isNotNull()) & (F.col("rate_distance") >= 2.0))
    .select("id")
)

tariff_rows_2 = df_tariff_windows.select("id")

result = tariff_rows.exceptAll(tariff_rows_2)
