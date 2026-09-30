from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh = Window.orderBy("kwh", "id")
w_all_by_rate = Window.orderBy(F.col("rate").asc_nulls_last(), "id")
w_quality_zone = Window.partitionBy("quality", "zone")

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

sequenced = (
    joined_ranges
    .select(
        "*",
        F.lag("read_at").over(w_all_by_kwh).alias("prev_read_at"),
        F.dense_rank().over(w_all_by_rate).alias("rate_dense_rank"),
        F.size(F.collect_set("site_id").over(w_quality_zone)).alias("distinct_site_id_in_quality_zone"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_zone), F.lit(0)), 4).alias("kwh_share_of_quality_zone"),
    )
)

joined_site = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") == "Z4"), "left")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    joined_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "rate",
        "energy_cost",
        "prev_read_at",
        "rate_dense_rank",
        "distinct_site_id_in_quality_zone",
        "site_commissioned_on",
        "site_installed_kw",
        F.when(F.col("zone").isin("Z4", "Z3"), "group_a").when(F.col("zone") == "Z2", "group_b").otherwise("other").alias("zone_group"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
    .orderBy(F.col("kwh").asc())
)
