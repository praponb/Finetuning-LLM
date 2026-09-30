from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy("kwh", "id")

valid = (
    df_meter_readings
    .filter(F.col("kwh").between(37.1, 92.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

screened = (
    valid.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 85.7), "left_semi")
)

latest_per_key = (
    screened
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .filter(F.col("kwh") > 52.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
    .orderBy("id", F.col("kwh").desc())
)
