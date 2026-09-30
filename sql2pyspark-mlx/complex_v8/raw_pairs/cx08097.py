from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_site_id = Window.partitionBy("zone", "site_id")
w_read_at_month_site_id_by_rat = Window.partitionBy(F.month("read_at"), "site_id").orderBy("rate", "id")
w_read_at_month_site_id_by_rat_2 = Window.partitionBy(F.month("read_at"), "site_id").orderBy("rate", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_site_id_by_rat_3 = Window.partitionBy(F.month("read_at"), "site_id").orderBy("rate", "id").rowsBetween(-1, -1)

joined_ranges = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

with_windows = (
    joined_ranges
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "rate",
        F.min("kwh").over(w_zone_site_id).alias("zone_site_id_min_kwh"),
        F.sum("kwh").over(w_zone_site_id).alias("zone_site_id_sum_kwh"),
        F.round(F.cume_dist().over(w_read_at_month_site_id_by_rat), 3).alias("rate_cume_dist"),
        F.min("rate").over(w_read_at_month_site_id_by_rat_2).alias("min_rate_so_far"),
        F.max("voltage").over(w_read_at_month_site_id_by_rat_3).alias("voltage_max_1_prior"),
    )
)

stats = (
    with_windows
    .groupBy(
        F.to_date("read_at").alias("read_at_day"),
        "site_id",
        F.when(F.col("rate_cume_dist") >= 0.8, "large").when(F.col("rate_cume_dist") >= 0.2, "medium").otherwise("small").alias("rate_cume_dist_tier"),
    )
    .agg(
        F.round(F.sum("voltage_max_1_prior"), 2).alias("total_voltage_max_1_prior"),
        F.round(F.sum("rate") / F.nullif(F.sum("zone_site_id_sum_kwh"), F.lit(0)), 3).alias("rate_to_zone_site_id_sum_kwh_ratio"),
        F.round(F.stddev("rate_cume_dist"), 2).alias("stddev_rate_cume_dist"),
        F.min("read_at").alias("first_read_at"),
        F.percentile_approx("zone_site_id_min_kwh", 0.5).alias("median_zone_site_id_min_kwh"),
    )
)

result = (
    stats
    .select(
        "read_at_day",
        "site_id",
        "rate_cume_dist_tier",
        "total_voltage_max_1_prior",
        "rate_to_zone_site_id_sum_kwh_ratio",
        "stddev_rate_cume_dist",
        "first_read_at",
        "median_zone_site_id_min_kwh",
        F.coalesce(F.col("stddev_rate_cume_dist"), F.lit(0.0)).alias("stddev_rate_cume_dist_filled"),
    )
)
