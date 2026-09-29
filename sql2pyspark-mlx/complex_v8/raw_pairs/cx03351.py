from pyspark.sql import functions as F

scoped = (
    df_tariff_windows
    .filter((F.hour(F.col("valid_from")).between(9, 11)) & (F.col("tariff") == "TOU"))
)

tariff_rows = scoped.select(F.col("id").alias("key_id"), "tariff", F.col("rate").alias("value"))

reading_side = (
    df_meter_readings
    .filter((F.col("kwh") >= 37.0) & (F.col("voltage") <= 236.7))
    .select(
        F.col("id").alias("key_id"),
        F.col("quality").alias("tariff"),
        F.col("voltage").alias("value"),
    )
)

result = (
    tariff_rows
    .unionByName(reading_side)
    .distinct()
    .orderBy(F.col("key_id").desc(), "value")
)
