from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("reading_ts_year", "id")
w2 = Window.partitionBy(F.date_trunc("week", "reading_ts"), "device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 08:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        F.year("reading_ts").alias("reading_ts_year"),
    )
)

result = (
    base
    .select(
        "id",
        "firmware",
        F.lead("humidity", 2).over(w).alias("next_humidity"),
        F.sum("reading_ts_year").over(w2).alias("cumulative_reading_ts_year"),
    )
)
