from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh_ties = Window.orderBy("kwh")
w_site_id = Window.partitionBy("site_id")

recent_readings = (
    df_meter_readings
    .filter((F.col("voltage") <= 234.7) & (F.col("quality") != "EST"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 0.9, 2).alias("kwh_adj"),
    )
)

with_windows = (
    recent_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh_adj",
        F.round(F.percent_rank().over(w_all_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.max("kwh").over(w_site_id).alias("site_id_max_kwh"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
        F.round(F.col("kwh_percent_rank") * 0.9, 2).alias("kwh_percent_rank_adj"),
    )
)

enriched = (
    tagged.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.year(F.col("si.commissioned_on")) == 2025), "left")
    .select(
        "s.*",
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

annotated = (
    enriched
    .select(
        "*",
        (F.floor(F.col("site_id_max_kwh") / 250) * 250).alias("site_id_max_kwh_bucket"),
    )
)

filtered = (
    annotated.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh_adj",
        "rows_in_site_id",
        "site_id_max_kwh",
        "kwh_percent_rank_adj",
        "site_tariff",
        "site_name",
        "site_installed_kw",
    )
)
