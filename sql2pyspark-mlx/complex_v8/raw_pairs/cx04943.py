from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_kwh_full = Window.partitionBy("quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id = Window.partitionBy("site_id")
w_quality_site_id_by_kwh_full = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_full = Window.orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_kwh = Window.partitionBy("quality").orderBy(F.col("kwh").desc(), "id")
w_all_by_site_commissioned_on = Window.orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("kwh") >= 52.4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

with_details_site = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (~F.col("si.zone").isin("Z2", "Z3")), "inner")
    .filter(F.col("si.commissioned_on").between(F.lit("2024-09-15").cast("date"), F.lit("2025-05-10").cast("date")))
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.read_at_label",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

ranked = (
    with_details_site
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "site_commissioned_on",
        F.nth_value("site_commissioned_on", 2).over(w_quality_by_kwh_full).alias("second_site_commissioned_on"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.nth_value("site_commissioned_on", 2).over(w_quality_site_id_by_kwh_full).alias("second_site_commissioned_on_2"),
        F.max("kwh").over(w_all_by_kwh_running).alias("max_kwh_so_far"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.nth_value("kwh", 2).over(w_all_by_kwh_full).alias("second_kwh"),
        F.lag("kwh", 1, 0).over(w_quality_by_kwh).alias("prev_kwh"),
        F.last("site_commissioned_on").over(w_all_by_kwh_full).alias("last_site_commissioned_on"),
        F.first("site_commissioned_on", ignorenulls=True).over(w_all_by_site_commissioned_on).alias("first_known_site_commissioned_on"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_id",
        "kwh",
        "site_commissioned_on",
        "second_site_commissioned_on",
        "kwh_share_of_site_id",
        "max_kwh_so_far",
        "kwh_vs_site_id_avg",
        "second_kwh",
        "prev_kwh",
        "last_site_commissioned_on",
        "first_known_site_commissioned_on",
        F.when(F.col("quality").isin("OK", "BAD"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
    )
)
