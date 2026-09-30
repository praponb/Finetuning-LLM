from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_days_since_read_a = Window.partitionBy("site_id").orderBy("days_since_read_at")
w_site_id = Window.partitionBy("site_id")
w_site_id_by_kwh_running = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month = Window.partitionBy(F.date_trunc("month", "read_at"))
w_site_id_by_days_since_read_a_2 = Window.partitionBy("site_id").orderBy("days_since_read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_days_since_read_a_3 = Window.partitionBy("site_id").orderBy(F.col("days_since_read_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_by_days_since_read_a_4 = Window.partitionBy("site_id").orderBy(F.col("days_since_read_at").desc())
w_site_id_by_days_since_read_a_5 = Window.partitionBy("site_id").orderBy("days_since_read_at", "id")
w_site_id_by_read_at_prior_1 = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(-1, -1)

recent_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) | (F.col("kwh") <= 92.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.round(F.col("voltage") * 0.9, 2).alias("voltage_adj"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

with_windows = (
    recent_readings
    .select(
        "*",
        F.round(F.cume_dist().over(w_site_id_by_days_since_read_a), 3).alias("days_since_read_at_cume_dist"),
        F.first("kwh", ignorenulls=True).over(w_site_id_by_kwh_running).alias("first_known_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_month), 2).alias("kwh_vs_read_at_month_avg"),
        F.first("voltage_adj").over(w_site_id_by_days_since_read_a_2).alias("first_voltage_adj"),
        F.max("days_since_read_at").over(w_site_id_by_days_since_read_a_3).alias("days_since_read_at_max_7"),
        F.ntile(10).over(w_site_id_by_read_at).alias("read_at_decile"),
        F.round(F.cume_dist().over(w_site_id_by_days_since_read_a_4), 3).alias("days_since_read_at_cume_dist_2"),
        F.lead("read_at", 3).over(w_site_id_by_days_since_read_a_5).alias("next3_read_at"),
        F.round(F.avg("days_since_read_at").over(w_site_id_by_read_at_prior_1), 2).alias("days_since_read_at_avg_1_prior"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.size(F.collect_set("site_id").over(w_read_at_month)).alias("distinct_site_id_in_read_at_month"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.date_format(F.col("next3_read_at"), "MMM yyyy").alias("next3_read_at_label"),
        F.coalesce(F.col("read_at_decile"), F.lit(0)).alias("read_at_decile_filled"),
        F.least(F.col("first_known_kwh"), F.col("voltage_adj")).alias("least_first_known_kwh_voltage_adj"),
    )
)

result = (
    tagged
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage_adj",
        "days_since_read_at",
        "days_since_read_at_cume_dist",
        "first_known_kwh",
        "kwh_vs_read_at_month_avg",
        "first_voltage_adj",
        "read_at_decile",
        "days_since_read_at_cume_dist_2",
        "days_since_read_at_avg_1_prior",
        "distinct_site_id_in_read_at_month",
        "read_at_decile_filled",
        F.initcap(F.trim(F.col("next3_read_at_label"))).alias("next3_read_at_label_clean"),
        F.round(F.col("rows_in_site_id") / F.nullif(F.col("kwh_vs_read_at_month_avg"), F.lit(0)), 3).alias("rows_in_site_id_per_kwh_vs_read_at_month_avg"),
    )
    .orderBy(F.col("read_at_decile").asc_nulls_last())
)
