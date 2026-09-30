from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_kwh_tier_by_read_at = Window.partitionBy("kwh_tier").orderBy(F.col("read_at").desc(), "id")
w_site_commissioned_on_year = Window.partitionBy(F.year("site_commissioned_on"))
w_all_by_read_at = Window.orderBy("read_at", "id")
w_read_at_month = Window.partitionBy(F.month("read_at"))
w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on").desc_nulls_first(), "id")
w_kwh_tier_by_read_at_full = Window.partitionBy("kwh_tier").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_readings = (
    df_meter_readings
    .filter(F.col("quality").isin("OK", "EST"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 80.8, "A").when(F.col("kwh") >= 43.1, "B").otherwise("C").alias("kwh_tier"),
    )
)

combined_details = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.kwh_tier",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.lag("read_at").over(w_kwh_tier_by_read_at).alias("prev_read_at"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_commissioned_on_year), 2).alias("voltage_vs_site_commissioned_on_year_avg"),
        F.lag("read_at", 3).over(w_all_by_read_at).alias("prev3_read_at"),
        F.count(F.lit(1)).over(w_read_at_month).alias("rows_in_read_at_month"),
        F.size(F.collect_set("site_zone").over(w_read_at_month)).alias("distinct_site_zone_in_read_at_month"),
        F.lag("voltage").over(w_all_by_site_commissioned_on).alias("prev_voltage"),
        F.min("voltage").over(w_read_at_month).alias("read_at_month_min_voltage"),
        F.nth_value("site_commissioned_on", 2).over(w_kwh_tier_by_read_at_full).alias("second_site_commissioned_on"),
    )
)

labeled = (
    ranked
    .filter(F.col("site_id") >= 5)
    .select(
        "*",
        F.concat_ws(": ", F.col("kwh_tier"), F.col("site_zone")).alias("kwh_tier_site_zone_label"),
        F.lower(F.regexp_replace(F.trim(F.col("kwh_tier")), " ", "_")).alias("kwh_tier_slug"),
    )
)

result = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh_tier",
        "site_zone",
        "prev_read_at",
        "voltage_vs_site_commissioned_on_year_avg",
        "rows_in_read_at_month",
        "distinct_site_zone_in_read_at_month",
        "prev_voltage",
        "second_site_commissioned_on",
    )
)
