from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_kwh = Window.partitionBy("quality", "site_id").orderBy("kwh", "id")
w_read_at_month_by_energy_cost = Window.partitionBy(F.month("read_at")).orderBy(F.col("energy_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

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

screened = (
    joined_ranges.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
    .filter(F.col("zone").isNotNull())
)

windowed = (
    screened
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "rate",
        F.lag("rate").over(w_quality_site_id_by_kwh).alias("prev_rate"),
        F.last("read_at").over(w_read_at_month_by_energy_cost).alias("last_read_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "kwh",
        "rate",
        "last_read_at",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
    .orderBy(F.col("rate").desc_nulls_first(), "kwh")
)
