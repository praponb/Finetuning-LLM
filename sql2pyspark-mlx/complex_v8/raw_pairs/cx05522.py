from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_month_by_com = Window.partitionBy(F.date_trunc("month", "commissioned_on")).orderBy(F.col("commissioned_on_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_commissioned_on_year = Window.partitionBy(F.year("first_commissioned_on"))
w_all_by_commissioned_on_movin = Window.orderBy("commissioned_on", "id").rowsBetween(-1, Window.currentRow)

base = (
    df_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("D%")) | (F.col("tariff") != "FLAT"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        F.date_trunc("month", F.col("commissioned_on")).alias("commissioned_on_month"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
        F.year(F.col("commissioned_on")).alias("commissioned_on_year"),
    )
)

qualified = (
    base
    .crossJoin(base.agg(F.min("commissioned_on_year").alias("overall_min_commissioned_on_year")))
    .filter(F.col("commissioned_on_year") > F.col("overall_min_commissioned_on_year"))
    .drop("overall_min_commissioned_on_year")
)

metrics = (
    qualified
    .withColumn("second_commissioned_on", F.nth_value("commissioned_on", 2).over(w_commissioned_on_month_by_com))
    .withColumn("first_commissioned_on", F.first("commissioned_on").over(w_commissioned_on_month_by_com))
)

with_windows = (
    metrics
    .select(
        "id",
        "site_name",
        "tariff",
        "commissioned_on",
        "commissioned_on_month",
        "days_since_commissioned_on",
        "commissioned_on_year",
        "second_commissioned_on",
        F.max("days_since_commissioned_on").over(w_first_commissioned_on_year).alias("first_commissioned_on_year_max_days_since_commis"),
        F.size(F.collect_set("zone").over(w_first_commissioned_on_year)).alias("distinct_zone_in_first_commissioned_on_year"),
        F.sum("days_since_commissioned_on").over(w_all_by_commissioned_on_movin).alias("days_since_commissioned_on_sum_2"),
        F.round(F.col("days_since_commissioned_on") - F.avg("days_since_commissioned_on").over(w_first_commissioned_on_year), 2).alias("days_since_commissioned_on_vs_first_commissioned"),
        F.size(F.collect_set("tariff").over(w_first_commissioned_on_year)).alias("distinct_tariff_in_first_commissioned_on_year"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

with_stats = (
    with_windows.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

labeled = (
    with_stats
    .filter(F.col("second_commissioned_on") >= F.lit("2024-10-28").cast("date"))
    .select(
        "*",
        F.concat_ws("-", F.col("tariff"), F.col("site_name")).alias("tariff_site_name_label"),
        F.round((F.unix_timestamp(F.col("commissioned_on_month")) - F.unix_timestamp(F.col("last_reading_read_at"))) / 60, 1).alias("minutes_last_reading_read_at_to_commissioned_on_"),
    )
)

result = (
    labeled
    .select(
        "id",
        "site_name",
        "tariff",
        "commissioned_on",
        "commissioned_on_year",
        "second_commissioned_on",
        "first_commissioned_on_year_max_days_since_commis",
        "distinct_zone_in_first_commissioned_on_year",
        "days_since_commissioned_on_sum_2",
        "days_since_commissioned_on_vs_first_commissioned",
        "last_reading_read_at",
        "tariff_site_name_label",
        "minutes_last_reading_read_at_to_commissioned_on_",
        F.when(F.col("tariff").isin("EV", "TOU"), "group_a").when(F.col("tariff") == "FLAT", "group_b").otherwise("other").alias("tariff_group"),
    )
    .orderBy("second_commissioned_on", "id")
    .limit(10)
)
