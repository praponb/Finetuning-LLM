from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_quality_by_kwh = Window.partitionBy("zone", "quality").orderBy(F.col("kwh").desc(), "id")
w_zone_quality_by_kwh_prior_6 = Window.partitionBy("zone", "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(-6, -1)
w_zone_quality_by_kwh_prior_1 = Window.partitionBy("zone", "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(-1, -1)
w_site_id = Window.partitionBy("site_id")
w_zone_quality_by_kwh_full = Window.partitionBy("zone", "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone_site_id_by_read_at_prio = Window.partitionBy("zone", "site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(-2, -1)

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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

screened = (
    joined_ranges.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.hour(F.col("m.read_at")).between(8, 14)), "left_anti")
)

ranked = (
    screened
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "zone",
        "rate",
        "energy_cost",
        F.lead("kwh").over(w_zone_quality_by_kwh).alias("next_kwh"),
        F.round(F.avg("kwh").over(w_zone_quality_by_kwh_prior_6), 2).alias("kwh_avg_6_prior"),
        F.max("rate").over(w_zone_quality_by_kwh_prior_1).alias("rate_max_1_prior"),
        F.max("energy_cost").over(w_site_id).alias("site_id_max_energy_cost"),
        F.last("rate").over(w_zone_quality_by_kwh_full).alias("last_rate"),
        F.sum("kwh").over(w_zone_site_id_by_read_at_prio).alias("kwh_sum_2_prior"),
    )
)

screened_2 = (
    ranked.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone").isin("Z4", "Z2")), "left_anti")
)

result = (
    screened_2
    .filter(F.col("site_id") <= 3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "zone",
        "rate",
        "next_kwh",
        "kwh_avg_6_prior",
        "rate_max_1_prior",
        "last_rate",
        "kwh_sum_2_prior",
        F.year(F.col("read_at")).alias("read_at_year"),
    )
    .orderBy("read_at", F.col("next_kwh").desc_nulls_first())
)
