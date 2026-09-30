from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .select(
        F.lit("tariff").alias("source"),
        "id",
        "rate",
        "tariff",
    )
)

tariff_side_2 = (
    df_tariff_windows
    .filter((F.to_date(F.col("valid_to")) < F.lit("2025-03-09").cast("date")) & (F.hour(F.col("valid_from")).between(6, 9)))
    .select(
        F.lit("tariff_2").alias("source"),
        "id",
        "rate",
        "tariff",
    )
)

unioned = tariff_side.union(tariff_side_2)

result = (
    unioned
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("source")
)
