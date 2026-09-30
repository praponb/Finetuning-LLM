from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_read_at_month_by_kwh = Window.partitionBy("site_id", F.date_trunc("month", "read_at")).orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_full = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_year = Window.partitionBy(F.year("read_at"))
w_site_zone = Window.partitionBy("site_zone")
w_site_id_read_at_month_by_kwh_2 = Window.partitionBy("site_id", F.date_trunc("month", "read_at")).orderBy("kwh", "id").rowsBetween(-3, Window.currentRow)
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_site_id_read_at_month_by_kwh_3 = Window.partitionBy("site_id", F.date_trunc("month", "read_at")).orderBy("kwh", "id").rowsBetween(-6, -1)
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc())

base = (
    df_meter_readings
    .filter((F.col("site_id") <= 2) | (F.col("kwh") <= 109.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

enriched_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.zone").alias("site_zone"))
)

metrics = (
    enriched_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        F.last("read_at", ignorenulls=True).over(w_all_by_read_at_running).alias("last_known_read_at"),
        F.first("voltage", ignorenulls=True).over(w_site_id_read_at_month_by_kwh).alias("first_known_voltage"),
        F.nth_value("kwh", 2).over(w_all_by_read_at_full).alias("second_kwh"),
        F.size(F.collect_set("site_zone").over(w_read_at_year)).alias("distinct_site_zone_in_read_at_year"),
        F.count(F.lit(1)).over(w_read_at_year).alias("rows_in_read_at_year"),
        F.count(F.lit(1)).over(w_site_zone).alias("rows_in_site_zone"),
        F.round(F.avg("kwh").over(w_site_id_read_at_month_by_kwh_2), 2).alias("kwh_avg_4"),
        F.dense_rank().over(w_all_by_read_at).alias("read_at_dense_rank"),
        F.lead("kwh", 2).over(w_all_by_kwh).alias("next2_kwh"),
        F.max("kwh").over(w_site_id_read_at_month_by_kwh_3).alias("kwh_max_6_prior"),
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_zone), 2).alias("kwh_vs_site_zone_avg"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "read_at",
        "kwh",
        "first_known_voltage",
        "distinct_site_zone_in_read_at_year",
        "rows_in_read_at_year",
        "rows_in_site_zone",
        "kwh_avg_4",
        "next2_kwh",
        "read_at_percent_rank",
        "kwh_vs_site_zone_avg",
        F.greatest(F.col("first_known_voltage"), F.col("second_kwh")).alias("greatest_first_known_voltage_second_kwh"),
        (F.col("rows_in_site_zone") - F.col("kwh_vs_site_zone_avg")).alias("rows_in_site_zone_minus_kwh_vs_site_zone_avg"),
    )
)

totals = (
    scored
    .filter(F.col("first_known_voltage") >= 223.0)
    .groupBy("distinct_site_zone_in_read_at_year")
    .agg(
        F.max("kwh_vs_site_zone_avg").alias("max_kwh_vs_site_zone_avg"),
        F.percentile_approx("greatest_first_known_voltage_second_kwh", 0.5).alias("median_greatest_first_known_voltage_second_kwh"),
    )
)

result = (
    totals
    .select(
        "distinct_site_zone_in_read_at_year",
        "max_kwh_vs_site_zone_avg",
        "median_greatest_first_known_voltage_second_kwh",
    )
)
