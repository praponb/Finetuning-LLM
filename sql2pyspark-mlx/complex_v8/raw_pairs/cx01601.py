from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("voltage") < 231.6)
    .select(
        "site_id",
        F.col("voltage").alias("metric"),
        F.lit("reading").alias("origin"),
    )
)

tariff_rows = df_tariff_windows.select("id", "rate", F.lit("tariff").alias("origin"))

unioned = (
    reading_side
    .union(tariff_rows)
    .distinct()
)

result = (
    unioned
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("metric").alias("total_metric"))
    .orderBy("origin")
)
