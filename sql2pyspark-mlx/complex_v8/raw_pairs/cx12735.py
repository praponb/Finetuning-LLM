from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_site_comm = Window.partitionBy("site_id", "quality").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(-1, Window.currentRow)
w_site_id_quality_by_site_comm_2 = Window.partitionBy("site_id", "quality").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_commissioned_on_year = Window.partitionBy(F.year("site_commissioned_on"))
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy("voltage", "id")
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_read_at_year = Window.partitionBy("quality", F.year("read_at"))
w_all_by_voltage_full = Window.orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality_by_site_comm_3 = Window.partitionBy("site_id", "quality").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_voltage_running = Window.partitionBy("quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_site_id_in_quality = Window.partitionBy("distinct_site_id_in_quality_read_at_year", "site_id")
w_site_id_quality_by_voltage_s = Window.partitionBy("site_id", "quality").orderBy("voltage_share_of_site_commissioned_on_year", "id").rowsBetween(-1, Window.currentRow)

prepared_readings = (
    df_meter_readings
    .filter(F.col("kwh") < 109.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

with_details = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.days_since_read_at",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.max("site_installed_kw").over(w_site_id_quality_by_site_comm).alias("site_installed_kw_max_2"),
        F.last("voltage", ignorenulls=True).over(w_site_id_quality_by_site_comm_2).alias("last_known_voltage"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_commissioned_on_year), 2).alias("voltage_vs_site_commissioned_on_year_avg"),
        F.lag("voltage").over(w_site_id_by_voltage).alias("prev_voltage"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_commissioned_on_year), F.lit(0)), 4).alias("voltage_share_of_site_commissioned_on_year"),
        F.max("days_since_read_at").over(w_all_by_voltage_running).alias("max_days_since_read_at_so_far"),
        F.size(F.collect_set("site_id").over(w_quality_read_at_year)).alias("distinct_site_id_in_quality_read_at_year"),
        F.last("site_installed_kw").over(w_all_by_voltage_full).alias("last_site_installed_kw"),
        F.nth_value("voltage", 2).over(w_site_id_quality_by_site_comm_3).alias("second_voltage"),
        F.max("site_installed_kw").over(w_quality_by_voltage_running).alias("max_site_installed_kw_so_far"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "days_since_read_at",
        "site_commissioned_on",
        "last_known_voltage",
        "voltage_vs_site_commissioned_on_year_avg",
        "prev_voltage",
        "voltage_share_of_site_commissioned_on_year",
        "distinct_site_id_in_quality_read_at_year",
        "last_site_installed_kw",
        "second_voltage",
        "max_site_installed_kw_so_far",
        F.count(F.lit(1)).over(w_distinct_site_id_in_quality).alias("rows_in_distinct_site_id_in_quality_read_at_year"),
        F.round(F.avg("prev_voltage").over(w_site_id_quality_by_voltage_s), 2).alias("prev_voltage_avg_2"),
        F.round(F.avg("site_installed_kw_max_2").over(w_site_id_quality_by_voltage_s), 2).alias("site_installed_kw_max_2_avg_2"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_distinct_site_id_in_quality), 2).alias("voltage_vs_distinct_site_id_in_quality_read_at_y"),
    )
)

summary = (
    sequenced
    .groupBy(
        F.to_date("read_at").alias("read_at_day"),
        F.date_trunc("month", "site_commissioned_on").alias("site_commissioned_on_month"),
    )
    .agg(
        F.min("voltage_share_of_site_commissioned_on_year").alias("min_voltage_share_of_site_commissioned_on_year"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("last_known_voltage"), 2).alias("total_last_known_voltage"),
    )
)

result = (
    summary
    .select(
        "read_at_day",
        "min_voltage_share_of_site_commissioned_on_year",
        "total_last_known_voltage",
    )
    .orderBy(
        F.col("read_at_day").desc(),
        F.col("total_last_known_voltage").asc_nulls_last(),
    )
)
