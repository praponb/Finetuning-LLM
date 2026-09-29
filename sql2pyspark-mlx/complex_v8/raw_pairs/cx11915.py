from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_rate_full = Window.orderBy("rate", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id = Window.partitionBy("site_id")
w_all_by_rate = Window.orderBy("rate", "id")

covered = (
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

windowed = (
    covered
    .select(
        "id",
        "site_id",
        "quality",
        "zone",
        "rate",
        "energy_cost",
        F.nth_value("energy_cost", 2).over(w_all_by_rate_full).alias("second_energy_cost"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_site_id), F.lit(0)), 4).alias("rate_share_of_site_id"),
        F.lag("kwh").over(w_all_by_rate).alias("prev_kwh"),
    )
)

result = (
    windowed
    .select(
        "id",
        "zone",
        "rate",
        "energy_cost",
        "second_energy_cost",
        "prev_kwh",
    )
    .orderBy(F.col("prev_kwh").desc())
)
