from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy("kwh", "id")

prepared_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage") <= 236.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

with_activity = (
    prepared_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") <= 241.8) & (F.col("m.kwh") > 34.1), "left_anti")
)

scored = (
    with_activity
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_slug",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_slug",
        "days_since_read_at",
        F.substring(F.trim(F.col("quality_slug")), 1, 2).alias("quality_slug_prefix"),
    )
    .orderBy("days_since_read_at")
)
