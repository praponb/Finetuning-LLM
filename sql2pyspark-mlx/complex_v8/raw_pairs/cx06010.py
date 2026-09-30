from pyspark.sql import functions as F

scoped = (
    df_tariff_windows
    .filter((F.col("tariff").isNotNull()) | (F.month(F.col("valid_to")).isin(1, 3)))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.lower(F.trim(F.col("tariff"))).alias("tariff_clean"),
    )
)

tariff_side = (
    scoped
    .filter((F.col("rate") < 5.5) & (F.col("valid_from") < F.lit("2025-02-16 11:00:00").cast("timestamp")))
    .select("tariff", "valid_from")
)

tariff_side_2 = df_tariff_windows.select("tariff", "valid_from")

result = (
    tariff_side
    .union(tariff_side_2)
    .distinct()
    .orderBy(F.col("valid_from").desc(), "tariff")
)
