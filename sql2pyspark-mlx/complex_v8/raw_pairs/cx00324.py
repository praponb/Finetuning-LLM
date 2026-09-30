from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_site_id = Window.partitionBy("site_id")
w_all_by_read_at_prior_1 = Window.orderBy("read_at", "id").rowsBetween(-1, -1)
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_full = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_full = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at_ties = Window.orderBy("read_at")
w_quality_by_read_at_running = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage = Window.orderBy("voltage", "id")

base = (
    df_meter_readings
    .filter((F.col("site_id") <= 9) & (F.col("quality") != "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_details_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_trends = (
    with_details_site
    .select(
        "*",
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.avg("voltage").over(w_all_by_read_at_prior_1), 2).alias("voltage_avg_1_prior"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality), F.lit(0)), 4).alias("kwh_share_of_quality"),
        F.last("read_at", ignorenulls=True).over(w_all_by_read_at_running).alias("last_known_read_at"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.nth_value("voltage", 2).over(w_all_by_read_at_full).alias("second_voltage"),
        F.first("kwh").over(w_all_by_voltage_full).alias("first_kwh"),
        F.dense_rank().over(w_all_by_read_at_ties).alias("read_at_dense_rank"),
        F.count(F.lit(1)).over(w_quality_by_read_at_running).alias("running_count"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.last("read_at").over(w_all_by_voltage_full).alias("last_read_at"),
        F.lag("kwh").over(w_all_by_voltage).alias("prev_kwh"),
    )
)

labeled = (
    with_trends
    .filter(F.col("distinct_site_id_in_quality") <= 8)
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "site_tariff",
        "kwh_vs_quality_avg",
        "rows_in_site_id",
        "voltage_avg_1_prior",
        "kwh_share_of_quality",
        "last_known_read_at",
        "kwh_share_of_site_id",
        "first_kwh",
        "read_at_dense_rank",
        "running_count",
        "distinct_site_id_in_quality",
        "last_read_at",
        F.round(F.col("kwh") / F.nullif(F.col("read_at_dense_rank"), F.lit(0)), 3).alias("kwh_per_read_at_dense_rank"),
    )
)

result = (
    labeled
    .filter(F.col("last_read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp"))
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "site_tariff",
        "kwh_vs_quality_avg",
        "rows_in_site_id",
        "kwh_share_of_site_id",
        "first_kwh",
        "read_at_dense_rank",
        "running_count",
        "distinct_site_id_in_quality",
        "last_read_at",
        "kwh_per_read_at_dense_rank",
    )
    .orderBy(F.col("last_read_at").desc_nulls_first(), "id")
    .limit(20)
)
