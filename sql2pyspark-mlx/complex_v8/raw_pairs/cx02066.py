from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_by_site_commission = Window.partitionBy("site_zone").orderBy(F.col("site_commissioned_on").desc())
w_quality_site_id_by_site_comm = Window.partitionBy("quality", "site_id").orderBy("site_commissioned_on")
w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on_2").desc(), "site_id", "quality").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id_by_site_comm_2 = Window.partitionBy("quality", "site_id").orderBy("site_commissioned_on", "site_id", "quality")

filtered_readings = (
    df_meter_readings
    .filter(F.col("kwh") >= 28.4)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

stats = (
    filtered_readings
    .groupBy("site_id", "quality")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.sum("voltage") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("voltage_to_kwh_ratio"),
        F.min("voltage").alias("min_voltage"),
    )
)

with_parent_site = (
    stats.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

joined = (
    with_parent_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.site_id",
        "s.quality",
        "s.quality_list",
        "s.site_commissioned_on",
        "s.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on_2"),
        F.col("si.zone").alias("site_zone"),
    )
)

ranked = (
    joined
    .select(
        "site_id",
        "quality",
        "quality_list",
        "site_name",
        "site_commissioned_on_2",
        "site_zone",
        F.rank().over(w_site_zone_by_site_commission).alias("site_commissioned_on_rank"),
        F.rank().over(w_quality_site_id_by_site_comm).alias("site_commissioned_on_rank_2"),
        F.last("site_commissioned_on_2").over(w_all_by_site_commissioned_on).alias("last_site_commissioned_on_2"),
        F.datediff(F.col("site_commissioned_on"), F.lag("site_commissioned_on").over(w_quality_site_id_by_site_comm_2)).alias("days_since_prev_site_commissioned_on"),
    )
)

screened = (
    ranked.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

enriched = (
    screened
    .select(
        "*",
        F.date_format(F.col("site_commissioned_on_2"), "MMM yyyy").alias("site_commissioned_on_2_label"),
        F.add_months(F.col("site_commissioned_on_2"), 3).alias("site_commissioned_on_2_plus_3m"),
    )
)

result = (
    enriched
    .groupBy("site_commissioned_on_2_plus_3m", "site_commissioned_on_rank_2")
    .pivot("site_zone", ["Z4", "Z3"])
    .agg(F.count("site_commissioned_on_rank").alias("count_v"), F.sum("site_commissioned_on_rank").alias("sum_v"))
    .orderBy("site_commissioned_on_2_plus_3m", "site_commissioned_on_rank_2")
)
