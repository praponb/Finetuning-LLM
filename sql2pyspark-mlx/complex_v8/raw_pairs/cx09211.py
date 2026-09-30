from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .select(
        F.col("site_id").alias("ref_id"),
        F.col("id").alias("qty"),
        F.col("voltage").alias("amount"),
        F.lit("reading").alias("origin"),
    )
)

reading_rows = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        F.lit("reading_2").alias("origin"),
    )
)

combined = reading_side.union(reading_rows)

result = (
    combined
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("origin")
)
