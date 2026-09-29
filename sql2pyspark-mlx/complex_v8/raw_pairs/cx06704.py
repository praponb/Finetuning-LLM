from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter((F.col("tariff").isin("FLAT", "TOU")) & (F.col("rate") < 5.9))
    .select(F.col("tariff").alias("category"), "valid_from", "rate")
)

reading_rows = (
    df_meter_readings
    .filter((F.col("voltage") > 220.2) & (F.month(F.col("read_at")).isin(1, 2)))
    .select(
        F.col("quality").alias("category"),
        F.col("read_at").alias("valid_from"),
        F.col("voltage").alias("rate"),
    )
)

result = (
    tariff_side
    .unionByName(reading_rows)
    .distinct()
    .orderBy("category")
)
