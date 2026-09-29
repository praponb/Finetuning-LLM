from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_read_at_t = Window.partitionBy("site_id", "quality").orderBy("read_at")
w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy("read_at", "id")

cleaned_readings = df_meter_readings.filter(F.col("quality") != "EST")

with_windows = (
    cleaned_readings
    .withColumn("read_at_cume_dist", F.round(F.cume_dist().over(w_site_id_quality_by_read_at_t), 3))
    .withColumn("voltage_change", F.col("voltage") - F.lag("voltage").over(w_site_id_quality_by_read_at))
)

joined_site = (
    with_windows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.read_at_cume_dist",
        "si.site_name",
    )
)

result = (
    joined_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "site_name",
    )
)
