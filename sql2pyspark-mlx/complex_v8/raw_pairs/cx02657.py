from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_quality_site_id_by_rate = Window.partitionBy("quality", "site_id").orderBy("rate", "id")

joined_ranges = (
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

qualified = (
    joined_ranges.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage").between(219.6, 237.5)) & (F.hour(F.col("m.read_at")).between(9, 13)), "left_anti")
)

sequenced = (
    qualified
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "zone",
        "rate",
        "energy_cost",
        F.size(F.collect_set("zone").over(w_quality_site_id)).alias("distinct_zone_in_quality_site_id"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_quality_site_id), F.lit(0)), 4).alias("rate_share_of_quality_site_id"),
        F.round(F.col("energy_cost") - F.avg("energy_cost").over(w_quality_site_id), 2).alias("energy_cost_vs_quality_site_id_avg"),
        F.lag("energy_cost").over(w_quality_site_id_by_rate).alias("prev_energy_cost"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["quality", "zone", "distinct_zone_in_quality_site_id"], ["quality"], []],
        "quality", "zone", "distinct_zone_in_quality_site_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("energy_cost_vs_quality_site_id_avg").alias("min_energy_cost_vs_quality_site_id_avg"),
        F.min("distinct_zone_in_quality_site_id").alias("min_distinct_zone_in_quality_site_id"),
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("zone").asc_nulls_last(),
        F.col("distinct_zone_in_quality_site_id").asc_nulls_last(),
    )
)
