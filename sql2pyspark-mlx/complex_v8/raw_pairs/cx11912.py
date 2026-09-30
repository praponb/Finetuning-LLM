from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_last_reading_read = Window.partitionBy("site_id").orderBy(F.col("last_reading_read_at").desc(), "id")
w_site_id_by_last_reading_read_2 = Window.partitionBy("site_id").orderBy(F.col("last_reading_read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_last_reading_read_3 = Window.partitionBy("site_id").orderBy(F.col("last_reading_read_at").desc())

prepared_readings = (
    df_meter_readings
    .filter(F.col("voltage") < 233.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

enriched_site = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

profiled = (
    enriched_site.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

with_windows = (
    profiled
    .select(
        "*",
        F.lag("last_reading_read_at").over(w_site_id_by_last_reading_read).alias("prev_last_reading_read_at"),
        F.first("site_commissioned_on", ignorenulls=True).over(w_site_id_by_last_reading_read_2).alias("first_known_site_commissioned_on"),
        F.lead("site_commissioned_on").over(w_site_id_by_last_reading_read).alias("next_site_commissioned_on"),
        F.round(F.percent_rank().over(w_site_id_by_last_reading_read), 3).alias("last_reading_read_at_percent_rank"),
        F.dense_rank().over(w_site_id_by_last_reading_read_3).alias("last_reading_read_at_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "quality",
        "site_commissioned_on",
        "prev_last_reading_read_at",
        "first_known_site_commissioned_on",
        "next_site_commissioned_on",
        "last_reading_read_at_percent_rank",
        "last_reading_read_at_dense_rank",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
    .orderBy("site_commissioned_on")
)
