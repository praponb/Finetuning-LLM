from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_by_kwh = Window.partitionBy("zone").orderBy(F.col("kwh").desc(), "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")

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

metrics = (
    in_window
    .select(
        "*",
        F.lag("rate", 3).over(w_zone_by_kwh).alias("prev3_rate"),
        F.dense_rank().over(w_zone_by_kwh).alias("kwh_dense_rank"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
    )
)

result = (
    metrics
    .filter(F.col("site_id") <= 8)
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "zone",
        "rate",
        "energy_cost",
        "prev3_rate",
        "kwh_dense_rank",
        "rows_in_quality_site_id",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.round(F.col("rate") / F.nullif(F.col("kwh_dense_rank"), F.lit(0)), 3).alias("rate_per_kwh_dense_rank"),
    )
    .orderBy(F.col("rate").desc(), "id")
    .limit(20)
)
