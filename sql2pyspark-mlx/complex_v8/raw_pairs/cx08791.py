from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id", "quality").orderBy("read_at", "id")

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

with_activity = (
    joined_ranges.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.to_date(F.col("m.read_at")) < F.lit("2025-02-06").cast("date")), "left_semi")
    .filter(F.col("quality") == "EST")
)

unique_rows = (
    with_activity
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

annotated = unique_rows.select("*", F.round(F.col("energy_cost") * 1.07, 2).alias("energy_cost_adj"))

result = (
    annotated
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "energy_cost",
        "energy_cost_adj",
        F.concat(F.col("zone"), F.lit("#"), F.col("id").cast("string")).alias("zone_ref"),
        F.least(F.col("rate"), F.col("energy_cost_adj")).alias("least_rate_energy_cost_adj"),
    )
)
