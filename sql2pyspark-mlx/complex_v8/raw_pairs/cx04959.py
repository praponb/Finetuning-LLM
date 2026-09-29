from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_kwh_ties = Window.partitionBy("quality").orderBy(F.col("kwh").desc())
w_quality_by_kwh_full = Window.partitionBy("quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").desc())
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy("voltage", "id")
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")

base = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

with_parent_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "si.site_name",
    )
)

metrics = (
    with_parent_site
    .select(
        "*",
        F.round(F.percent_rank().over(w_quality_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.rank().over(w_quality_by_kwh_ties).alias("kwh_rank"),
        F.last("voltage").over(w_quality_by_kwh_full).alias("last_voltage"),
        F.min("kwh").over(w_quality_site_id).alias("quality_site_id_min_kwh"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank_2"),
        F.round(F.percent_rank().over(w_site_id_by_voltage), 3).alias("voltage_percent_rank"),
        F.last("kwh").over(w_site_id_by_voltage_full).alias("last_kwh"),
        F.nth_value("voltage", 2).over(w_quality_by_kwh_full).alias("second_voltage"),
        F.row_number().over(w_all_by_kwh).alias("kwh_seq"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_site_id), 2).alias("kwh_vs_quality_site_id_avg"),
        F.min("voltage").over(w_quality_site_id).alias("quality_site_id_min_voltage"),
    )
)

result = (
    metrics
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "site_name",
        "kwh_rank_2",
        "last_kwh",
        "kwh_seq",
        "kwh_vs_quality_site_id_avg",
        "quality_site_id_min_voltage",
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
        F.abs(F.col("kwh_seq") - 5).alias("kwh_seq_distance"),
    )
    .orderBy(F.col("kwh_rank_2").desc(), F.col("site_name_slug").asc())
)
