from pyspark.sql import functions as F

scoped = (
    df_tariff_windows
    .filter((F.month(F.col("valid_from")).isin(2, 3)) | (F.col("valid_to") >= F.lit("2025-02-18 04:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

tariff_rows = (
    scoped
    .filter(F.month(F.col("valid_to")).isin(1, 2))
    .select("id", "rate", F.col("valid_to").alias("event_time"))
)

tariff_side = df_tariff_windows.select("id", "rate", F.col("valid_from").alias("event_time"))

tariff_side_2 = df_tariff_windows.select("id", "rate", F.col("valid_from").alias("event_time"))

result = (
    tariff_rows
    .unionByName(tariff_side)
    .unionByName(tariff_side_2)
    .orderBy("id", "event_time")
)
