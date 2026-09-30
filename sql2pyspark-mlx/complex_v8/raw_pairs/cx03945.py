from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month = Window.partitionBy(F.date_trunc("month", "read_at"))

recent = (
    df_meter_readings
    .filter(F.col("kwh") >= 16.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

ranked = (
    recent
    .select(
        "*",
        F.min("kwh").over(w_read_at_month).alias("read_at_month_min_kwh"),
        F.count(F.lit(1)).over(w_read_at_month).alias("rows_in_read_at_month"),
        F.max("voltage").over(w_read_at_month).alias("read_at_month_max_voltage"),
        F.size(F.collect_set("site_id").over(w_read_at_month)).alias("distinct_site_id_in_read_at_month"),
    )
)

exploded_detail_readings = (
    ranked.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.rows_in_read_at_month",
        "s.read_at_month_max_voltage",
        "s.distinct_site_id_in_read_at_month",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    exploded_detail_readings
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "rows_in_read_at_month",
        "read_at_month_max_voltage",
        "distinct_site_id_in_read_at_month",
        "reading_quality",
        "reading_kwh",
    )
    .orderBy(F.col("kwh").desc(), F.col("read_at_month_max_voltage").desc())
)
