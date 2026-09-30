from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")

filtered = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-03-02 11:00:00").cast("timestamp")) & (F.col("voltage") > 215.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.when(F.col("kwh") >= 83.8, "large").when(F.col("kwh") >= 30.2, "medium").otherwise("small").alias("kwh_tier"),
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.max("voltage").over(w_site_id_quality).alias("site_id_quality_max_voltage"),
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        F.size(F.collect_set("kwh_tier").over(w_site_id_quality)).alias("distinct_kwh_tier_in_site_id_quality"),
    )
)

detail_rows_readings = (
    sequenced.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality_or_unknown",
        "s.kwh_tier",
        "s.site_id_quality_max_voltage",
        "s.rows_in_site_id_quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    detail_rows_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh_tier",
        "site_id_quality_max_voltage",
        "rows_in_site_id_quality",
        "reading_id",
        "reading_kwh",
        "reading_voltage",
        "reading_read_at",
    )
    .orderBy("site_id_quality_max_voltage")
)
