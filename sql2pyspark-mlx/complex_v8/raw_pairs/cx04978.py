from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at_prior_1 = Window.partitionBy("site_id").orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_site_id = Window.partitionBy("site_id")

filtered_readings = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("voltage").alias("max_reading_voltage"))
)

with_stats = (
    filtered_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.max_reading_voltage")
)

with_windows = (
    with_stats
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "read_at_label",
        "max_reading_voltage",
        F.round(F.avg("kwh").over(w_site_id_by_read_at_prior_1), 2).alias("kwh_avg_1_prior"),
        F.max("max_reading_voltage").over(w_site_id).alias("site_id_max_max_reading_voltage"),
    )
)

result = (
    with_windows
    .filter(F.col("site_id") <= 4)
    .select(
        "id",
        "site_id",
        "read_at",
        "read_at_label",
        "max_reading_voltage",
        "kwh_avg_1_prior",
        "site_id_max_max_reading_voltage",
        F.concat(F.col("read_at_label"), F.lit("#"), F.col("site_id").cast("string")).alias("read_at_label_ref"),
        (F.floor(F.col("kwh_avg_1_prior") / 25) * 25).alias("kwh_avg_1_prior_bucket"),
    )
    .orderBy("site_id", "id")
    .limit(20)
)
