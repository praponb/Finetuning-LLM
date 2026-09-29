from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_read_at_year_by_volt = Window.partitionBy("quality", F.year("read_at")).orderBy("voltage")
w_all_by_site_commissioned_on_2 = Window.orderBy(F.col("site_commissioned_on").desc(), "id")
w_quality_read_at_year_by_volt_2 = Window.partitionBy("quality", F.year("read_at")).orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_read_at_year_by_volt_3 = Window.partitionBy("quality", F.year("read_at")).orderBy("voltage", "id")
w_quality_site_commissioned_on = Window.partitionBy("quality", F.year("site_commissioned_on"))
w_site_zone = Window.partitionBy("site_zone")
w_distinct_site_zone_in_qualit = Window.partitionBy("distinct_site_zone_in_quality_site_commissioned_", "site_zone").orderBy("distinct_site_zone_in_quality_site_commissioned_", "id")

cleaned_readings = df_meter_readings.filter((F.col("quality") == "BAD") & (F.col("voltage") <= 239.6))

combined_details_site = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

windowed = (
    combined_details_site
    .select(
        "*",
        F.first("kwh", ignorenulls=True).over(w_all_by_site_commissioned_on).alias("first_known_kwh"),
        F.round(F.percent_rank().over(w_quality_read_at_year_by_volt), 3).alias("voltage_percent_rank"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_site_commissioned_on_2)).alias("voltage_change"),
        F.first("voltage").over(w_quality_read_at_year_by_volt_2).alias("first_voltage"),
        F.lag("kwh").over(w_quality_read_at_year_by_volt_3).alias("prev_kwh"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_site_commissioned_on), F.lit(0)), 4).alias("kwh_share_of_quality_site_commissioned_on_year"),
        F.size(F.collect_set("site_zone").over(w_quality_site_commissioned_on)).alias("distinct_site_zone_in_quality_site_commissioned_"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "site_name",
        "site_zone",
        "site_commissioned_on",
        "first_known_kwh",
        "voltage_change",
        "first_voltage",
        "prev_kwh",
        "kwh_share_of_quality_site_commissioned_on_year",
        "distinct_site_zone_in_quality_site_commissioned_",
        F.round(F.col("first_known_kwh") - F.avg("first_known_kwh").over(w_site_zone), 2).alias("first_known_kwh_vs_site_zone_avg"),
        F.row_number().over(w_distinct_site_zone_in_qualit).alias("distinct_site_zone_in_quality_site_commissioned__2"),
        F.sum("voltage_change").over(w_site_zone).alias("site_zone_sum_voltage_change"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "read_at",
        "site_zone",
        "site_commissioned_on",
        "first_known_kwh",
        "first_voltage",
        "prev_kwh",
        "kwh_share_of_quality_site_commissioned_on_year",
        "distinct_site_zone_in_quality_site_commissioned_",
        "first_known_kwh_vs_site_zone_avg",
        "distinct_site_zone_in_quality_site_commissioned__2",
        F.concat(F.col("site_zone"), F.lit("#"), F.col("id").cast("string")).alias("site_zone_ref"),
    )
    .orderBy("distinct_site_zone_in_quality_site_commissioned_")
)
