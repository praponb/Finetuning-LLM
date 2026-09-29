from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter((F.month(F.col("valid_to")).isin(2, 3)) & (F.col("tariff").isin("EV", "TOU")))
    .select("tariff", F.col("rate").alias("amount"), "id")
)

reading_rows = (
    df_meter_readings
    .select(
        F.col("quality").alias("tariff"),
        F.col("voltage").alias("amount"),
        F.col("site_id").alias("id"),
    )
)

reading_rows_2 = (
    df_meter_readings
    .filter((F.col("voltage") >= 222.3) & (F.col("quality") != "EST"))
    .select(
        F.col("quality").alias("tariff"),
        F.col("voltage").alias("amount"),
        F.col("site_id").alias("id"),
    )
)

result = (
    tariff_side
    .union(reading_rows)
    .union(reading_rows_2)
)
