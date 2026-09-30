from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_site_id = Window.partitionBy("site_zone", "site_id")
w_quality_site_id_by_kwh = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc(), "id")

scoped = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(10, 14))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_parent_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select("s.*", F.col("si.zone").alias("site_zone"), "si.site_name")
)

with_trends = (
    with_parent_site
    .select(
        "*",
        F.count(F.lit(1)).over(w_site_zone_site_id).alias("rows_in_site_zone_site_id"),
        F.lead("read_at", 3).over(w_quality_site_id_by_kwh).alias("next3_read_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "site_zone",
        "rows_in_site_zone_site_id",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("next3_read_at")).alias("days_since_next3_read_at"),
    )
    .orderBy("rows_in_site_zone_site_id")
)
