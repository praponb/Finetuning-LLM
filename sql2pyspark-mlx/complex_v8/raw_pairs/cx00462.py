from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_commissioned_on_month = Window.partitionBy(F.month("site_commissioned_on"))
w_site_commissioned_on_year_re = Window.partitionBy(F.year("site_commissioned_on"), F.date_trunc("month", "read_at")).orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_voltage_tier_by_kwh = Window.partitionBy("site_id", "voltage_tier").orderBy("kwh", "id")

filtered = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("voltage") >= 236.7, "A").when(F.col("voltage") >= 220.7, "B").otherwise("C").alias("voltage_tier"),
    )
)

enriched_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

with_trends = (
    enriched_site
    .select(
        "*",
        F.sum("kwh").over(w_site_commissioned_on_month).alias("site_commissioned_on_month_sum_kwh"),
        F.nth_value("read_at", 2).over(w_site_commissioned_on_year_re).alias("second_read_at"),
        F.size(F.collect_set("site_id").over(w_site_commissioned_on_month)).alias("distinct_site_id_in_site_commissioned_on_month"),
        F.dense_rank().over(w_site_id_voltage_tier_by_kwh).alias("kwh_dense_rank"),
        F.min("kwh").over(w_site_commissioned_on_month).alias("site_commissioned_on_month_min_kwh"),
        F.round(F.percent_rank().over(w_site_id_voltage_tier_by_kwh), 3).alias("kwh_percent_rank"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("kwh").alias("total_reading_kwh"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

summarized_rows = (
    with_trends.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.total_reading_kwh", "m.first_reading_read_at")
)

rollup_base = (
    summarized_rows
    .filter(F.col("site_commissioned_on_month_min_kwh") < 780.0)
    .groupBy(
        "distinct_site_id_in_site_commissioned_on_month",
        F.date_trunc("month", "read_at").alias("read_at_month"),
    )
    .agg(
        F.countDistinct("voltage_tier").alias("distinct_voltage_tiers"),
        F.sum("distinct_site_id_in_site_commissioned_on_month").alias("total_distinct_site_id_in_site_commissioned_on_m"),
    )
)

result = (
    rollup_base
    .select(
        "distinct_site_id_in_site_commissioned_on_month",
        "read_at_month",
        "distinct_voltage_tiers",
    )
    .orderBy("distinct_voltage_tiers")
)
