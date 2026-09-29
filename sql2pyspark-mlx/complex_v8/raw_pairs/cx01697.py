from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), F.col("id").desc())

filtered_readings = (
    df_meter_readings
    .filter((F.col("voltage") >= 215.6) | (F.col("kwh") <= 81.3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

screened = (
    filtered_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.month(F.col("m.read_at")).isin(1, 3)) & (F.col("m.quality") != "OK"), "left_semi")
    .filter(F.col("voltage") > 219.6)
)

latest_per_key = (
    screened
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
    .orderBy("voltage", F.col("kwh").desc_nulls_first())
)
