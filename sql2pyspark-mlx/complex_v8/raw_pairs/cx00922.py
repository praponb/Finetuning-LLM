from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_pct_zone_z2 = Window.partitionBy("site_id").orderBy(F.col("pct_zone_z2").desc(), "read_at_month", "rate_tier", "site_id")
w_all_by_pct_zone_z2_prior_1 = Window.orderBy("pct_zone_z2", "read_at_month", "rate_tier", "site_id").rowsBetween(-1, -1)
w_rate_tier_by_max_voltage_ful = Window.partitionBy("rate_tier").orderBy(F.col("max_voltage").desc_nulls_first(), "read_at_month", "rate_tier", "site_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rate_tier = Window.partitionBy("rate_tier")
w_rate_tier_by_rate_zone_z3 = Window.partitionBy("rate_tier").orderBy(F.col("rate_zone_z3").desc(), "read_at_month", "rate_tier", "site_id")

matched = (
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

stats = (
    matched
    .groupBy(
        F.date_trunc("month", "read_at").alias("read_at_month"),
        F.when(F.col("rate") >= 5.7, "HIGH").when(F.col("rate") >= 4.2, "MEDIUM").otherwise("LOW").alias("rate_tier"),
        "site_id",
    )
    .agg(
        F.max("voltage").alias("max_voltage"),
        F.array_sort(F.collect_set("zone")).alias("zone_list"),
        F.sum(F.when(F.col("zone") == "Z3", F.col("rate")).otherwise(0)).alias("rate_zone_z3"),
        F.round(100 * F.count(F.when(F.col("zone") != "Z2", 1)) / F.count("*"), 1).alias("pct_zone_z2"),
    )
)

ranked = (
    stats
    .select(
        "read_at_month",
        "rate_tier",
        "site_id",
        "max_voltage",
        "zone_list",
        "rate_zone_z3",
        "pct_zone_z2",
        F.lag("max_voltage").over(w_site_id_by_pct_zone_z2).alias("prev_max_voltage"),
        F.sum("pct_zone_z2").over(w_all_by_pct_zone_z2_prior_1).alias("pct_zone_z2_sum_1_prior"),
        F.sum("rate_zone_z3").over(w_all_by_pct_zone_z2_prior_1).alias("rate_zone_z3_sum_1_prior"),
        F.round(F.cume_dist().over(w_site_id_by_pct_zone_z2), 3).alias("pct_zone_z2_cume_dist"),
        F.first("max_voltage").over(w_rate_tier_by_max_voltage_ful).alias("first_max_voltage"),
    )
)

sequenced = (
    ranked
    .select(
        "read_at_month",
        "rate_tier",
        "site_id",
        "max_voltage",
        "zone_list",
        "rate_zone_z3",
        "pct_zone_z2",
        "pct_zone_z2_sum_1_prior",
        "rate_zone_z3_sum_1_prior",
        "pct_zone_z2_cume_dist",
        F.max("first_max_voltage").over(w_rate_tier).alias("rate_tier_max_first_max_voltage"),
        F.size(F.collect_set("site_id").over(w_rate_tier)).alias("distinct_site_id_in_rate_tier"),
        F.round((F.unix_timestamp("read_at_month") - F.unix_timestamp(F.lag("read_at_month").over(w_rate_tier_by_rate_zone_z3))) / 60, 1).alias("minutes_since_prev_read_at_month"),
    )
)

result = (
    sequenced
    .filter(~F.col("rate_tier").isin("LOW", "MEDIUM"))
    .select(
        "read_at_month",
        "rate_tier",
        "site_id",
        "max_voltage",
        "rate_zone_z3",
        "pct_zone_z2_sum_1_prior",
        "pct_zone_z2_cume_dist",
        "rate_tier_max_first_max_voltage",
        "minutes_since_prev_read_at_month",
        F.when(F.col("rate_zone_z3") >= 32.0, "HIGH").when(F.col("rate_zone_z3") >= 14.0, "MEDIUM").otherwise("LOW").alias("rate_zone_z3_tier"),
        F.to_date(F.col("read_at_month")).alias("read_at_month_date"),
    )
    .orderBy("pct_zone_z2_cume_dist", "read_at_month")
)
