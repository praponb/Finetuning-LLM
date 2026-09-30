from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_site_id_by_kwh = Window.partitionBy(F.month("read_at"), "site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_zone = Window.partitionBy("site_zone")
w_site_tariff = Window.partitionBy("site_tariff")
w_site_id_by_read_at_moving_2 = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_read_at_month_site_id_by_kwh_2 = Window.partitionBy(F.month("read_at"), "site_id").orderBy("kwh", "id").rowsBetween(-2, -1)
w_quality_by_read_at_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .filter((F.col("voltage") < 240.2) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.when(F.col("kwh") >= 79.4, "HIGH").when(F.col("kwh") >= 48.2, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
    )
)

with_parent_site = (
    cleaned.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_trends = (
    with_parent_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "quality_ref",
        "kwh_tier",
        "site_zone",
        "site_tariff",
        F.first("kwh").over(w_read_at_month_site_id_by_kwh).alias("first_kwh"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_zone), F.lit(0)), 4).alias("kwh_share_of_site_zone"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_tariff), 2).alias("kwh_vs_site_tariff_avg"),
        F.round(F.avg("kwh").over(w_site_id_by_read_at_moving_2), 2).alias("kwh_avg_3"),
        F.sum("kwh").over(w_site_tariff).alias("site_tariff_sum_kwh"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.avg("kwh").over(w_read_at_month_site_id_by_kwh_2), 2).alias("kwh_avg_2_prior"),
        F.min("kwh").over(w_quality_by_read_at_running).alias("min_kwh_so_far"),
        F.size(F.collect_set("site_tariff").over(w_site_zone)).alias("distinct_site_tariff_in_site_zone"),
    )
)

totals = (
    with_trends
    .groupBy("kwh_tier", "site_zone", "site_tariff")
    .agg(
        F.round(F.avg("min_kwh_so_far"), 2).alias("avg_min_kwh_so_far"),
        F.round(F.avg("site_tariff_sum_kwh"), 2).alias("avg_site_tariff_sum_kwh"),
        F.max("site_tariff_sum_kwh").alias("max_site_tariff_sum_kwh"),
        F.count(F.when(F.col("site_zone") == "Z3", 1)).alias("n_site_zone_z3"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .select(
        "kwh_tier",
        "site_zone",
        "site_tariff",
        "avg_site_tariff_sum_kwh",
        "max_site_tariff_sum_kwh",
        "n_site_zone_z3",
        "cnt",
    )
    .orderBy(F.col("avg_site_tariff_sum_kwh").desc())
)
