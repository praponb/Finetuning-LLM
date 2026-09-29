from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last(), "id")

enriched = (
    df_device_readings
    .withColumn("corr_status_bits_humidity", F.corr("status_bits", "humidity").over(w))
    .withColumn("prev_pressure", F.lag("pressure", 1).over(w2))
    .withColumn("cumulative_humidity", F.sum("humidity").over(w3))
)

base = (
    enriched
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .withColumn("greatest_pressure_status_bits", F.greatest("pressure", "status_bits"))
    .withColumn("prev_pressure_minus_pressure", (F.col("prev_pressure") - F.col("pressure")))
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    latest
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "corr_status_bits_humidity",
        "prev_pressure",
        "cumulative_humidity",
        "greatest_pressure_status_bits",
        "prev_pressure_minus_pressure",
        (F.col("corr_status_bits_humidity") - F.col("prev_pressure_minus_pressure")).alias("corr_status_bits_humidity_minus_prev_pressure_minus_pressure"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("status_bits") >= 11, "PREMIUM").otherwise("STANDARD").alias("status_bits_tier"),
        F.when(F.col("corr_status_bits_humidity_minus_prev_pressure_minus_pressure") >= 63, "HIGH").otherwise("LOW").alias("corr_status_bits_humidity_minus_prev_pressure_minus_pressure_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.percentile_approx("prev_pressure_minus_pressure", 0.5).alias("prev_pressure_minus_pressure_p50"),
    )
    .orderBy(F.col("status_bits_tier").desc_nulls_first())
)
