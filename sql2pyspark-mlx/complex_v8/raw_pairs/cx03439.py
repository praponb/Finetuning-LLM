from pyspark.sql import functions as F

tariff_side = df_tariff_windows.select("tariff", "id", F.lit("tariff").alias("record_type"))

reading_rows = (
    df_meter_readings
    .filter(F.col("site_id") >= 3)
    .select("quality", "id", F.lit("reading").alias("record_type"))
)

unioned = tariff_side.union(reading_rows)

result = (
    unioned
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("record_type")
)
