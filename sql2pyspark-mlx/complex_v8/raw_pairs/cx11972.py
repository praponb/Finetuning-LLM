from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_full = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_quality = Window.partitionBy(F.month("read_at"), "quality")
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id")
w_all_by_read_at = Window.orderBy("read_at", "id")

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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

with_windows = (
    matched
    .select(
        "*",
        F.first("rate").over(w_all_by_read_at_full).alias("first_rate"),
        F.round(F.col("rate") - F.avg("rate").over(w_read_at_month_quality), 2).alias("rate_vs_read_at_month_quality_avg"),
        F.size(F.collect_set("site_id").over(w_read_at_month_quality)).alias("distinct_site_id_in_read_at_month_quality"),
        (F.col("energy_cost") - F.lag("energy_cost").over(w_site_id_by_kwh)).alias("energy_cost_change"),
        F.row_number().over(w_all_by_read_at).alias("rn"),
    )
)

result = (
    with_windows
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "zone",
        "rate",
        "energy_cost",
        "first_rate",
        "energy_cost_change",
        "rn",
        F.lower(F.trim(F.col("zone"))).alias("zone_clean"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
    .orderBy(F.col("energy_cost").desc_nulls_first(), "rate")
)
