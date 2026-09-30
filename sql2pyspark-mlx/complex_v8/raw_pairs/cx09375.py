from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_month_day_ye = Window.partitionBy(F.year("commissioned_on_month_day")).orderBy(F.col("distinct_tariffs_plus_reading_count_tariff_tou").desc(), "reading_count", "commissioned_on_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_commissioned_on_month_day_mo = Window.partitionBy(F.month("commissioned_on_month_day"), "distinct_zones")
w_commissioned_on_month_day_ye_2 = Window.partitionBy(F.year("commissioned_on_month_day")).orderBy(F.col("distinct_tariffs_plus_reading_count_tariff_tou").desc())
w_reading_count_by_distinct_zo = Window.partitionBy("reading_count").orderBy(F.col("distinct_zones").desc(), "reading_count", "commissioned_on_month")

prepared = (
    df_sites
    .filter(F.length(F.trim(F.col("site_name"))) >= 8)
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        F.concat_ws(" / ", F.col("tariff"), F.col("zone")).alias("tariff_zone_label"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_stats = (
    prepared.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

rollup_base = (
    with_stats
    .groupBy(
        "reading_count",
        F.date_trunc("month", "commissioned_on").alias("commissioned_on_month"),
    )
    .agg(
        F.countDistinct("tariff").alias("distinct_tariffs"),
        F.sum(F.when(F.col("tariff") == "TOU", F.col("reading_count")).otherwise(0)).alias("reading_count_tariff_tou"),
        F.max("reading_count").alias("max_reading_count"),
        F.countDistinct("zone").alias("distinct_zones"),
        F.avg("reading_count").alias("avg_reading_count_check"),
    )
    .filter(F.col("avg_reading_count_check") > 3)
    .drop("avg_reading_count_check")
)

derived = (
    rollup_base
    .select(
        "reading_count",
        "commissioned_on_month",
        "distinct_zones",
        (F.col("distinct_tariffs") + F.col("reading_count_tariff_tou")).alias("distinct_tariffs_plus_reading_count_tariff_tou"),
        F.date_trunc("day", F.col("commissioned_on_month")).alias("commissioned_on_month_day"),
    )
)

derived_2 = (
    derived
    .filter(F.col("distinct_zones") < 7)
    .select(
        "*",
        (F.col("distinct_zones") + F.col("reading_count")).alias("distinct_zones_plus_reading_count"),
    )
)

windowed = (
    derived_2
    .select(
        "*",
        F.first("distinct_zones", ignorenulls=True).over(w_commissioned_on_month_day_ye).alias("first_known_distinct_zones"),
        F.round(F.avg("distinct_zones").over(w_commissioned_on_month_day_mo), 2).alias("commissioned_on_month_day_month_distinct_zones_a"),
        F.dense_rank().over(w_commissioned_on_month_day_ye_2).alias("distinct_tariffs_plus_reading_count_tariff_tou_d"),
        F.count(F.lit(1)).over(w_commissioned_on_month_day_mo).alias("rows_in_commissioned_on_month_day_month_distinct"),
        F.lead("commissioned_on_month_day").over(w_reading_count_by_distinct_zo).alias("next_commissioned_on_month_day"),
        F.round(F.col("reading_count") - F.avg("reading_count").over(w_commissioned_on_month_day_mo), 2).alias("reading_count_vs_commissioned_on_month_day_month"),
    )
)

labeled = (
    windowed
    .select(
        "*",
        F.year(F.col("commissioned_on_month")).alias("commissioned_on_month_year"),
        (F.col("reading_count") - F.col("distinct_zones")).alias("reading_count_minus_distinct_zones"),
    )
)

result = (
    labeled
    .select(
        "reading_count",
        "commissioned_on_month",
        "distinct_zones",
        "commissioned_on_month_day",
        "distinct_zones_plus_reading_count",
        "first_known_distinct_zones",
        "commissioned_on_month_day_month_distinct_zones_a",
        "distinct_tariffs_plus_reading_count_tariff_tou_d",
        "rows_in_commissioned_on_month_day_month_distinct",
        "next_commissioned_on_month_day",
        "commissioned_on_month_year",
        "reading_count_minus_distinct_zones",
        (F.col("commissioned_on_month") + F.expr("INTERVAL 7 DAYS")).alias("commissioned_on_month_plus_7d"),
    )
)
