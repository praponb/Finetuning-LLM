from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_site_id = Window.partitionBy(F.month("read_at"), "site_id")
w_all_by_rate_ties = Window.orderBy("rate")
w_all_by_rate = Window.orderBy("rate", "id")
w_all_by_rate_full = Window.orderBy("rate", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_rate_dense_rank_site_id_by_f = Window.partitionBy("rate_dense_rank", "site_id").orderBy("first_rate")

in_window = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
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

ranked = (
    in_window
    .select(
        "*",
        F.round(F.col("energy_cost") / F.nullif(F.sum("energy_cost").over(w_read_at_month_site_id), F.lit(0)), 4).alias("energy_cost_share_of_read_at_month_site_id"),
        F.dense_rank().over(w_all_by_rate_ties).alias("rate_dense_rank"),
        F.lead("kwh").over(w_all_by_rate).alias("next_kwh"),
        F.first("rate").over(w_all_by_rate_full).alias("first_rate"),
        F.round(F.cume_dist().over(w_all_by_rate_ties), 3).alias("rate_cume_dist"),
    )
)

with_trends = (
    ranked
    .select(
        "*",
        F.sum("energy_cost").over(w_site_id_quality).alias("site_id_quality_sum_energy_cost"),
        F.round(F.cume_dist().over(w_rate_dense_rank_site_id_by_f), 3).alias("first_rate_cume_dist"),
        F.min("rate_dense_rank").over(w_site_id_quality).alias("site_id_quality_min_rate_dense_rank"),
        F.size(F.collect_set("zone").over(w_site_id_quality)).alias("distinct_zone_in_site_id_quality"),
    )
)

result = (
    with_trends
    .filter(F.col("first_rate_cume_dist") <= 0.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "zone",
        "energy_cost",
        "energy_cost_share_of_read_at_month_site_id",
        "next_kwh",
        "first_rate",
        "site_id_quality_sum_energy_cost",
        "first_rate_cume_dist",
        "distinct_zone_in_site_id_quality",
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
    .orderBy(F.col("quality_group").desc())
)
