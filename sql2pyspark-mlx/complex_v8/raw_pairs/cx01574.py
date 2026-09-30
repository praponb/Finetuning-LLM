from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_kwh_tier_zone = Window.partitionBy("kwh_tier", "zone")

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

derived = (
    covered
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.when(F.col("kwh") >= 81.1, "HIGH").when(F.col("kwh") >= 35.9, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
    )
)

with_windows = (
    derived
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "energy_cost",
        "quality_slug",
        "kwh_tier",
        "zone_or_unknown",
        F.round(F.avg("energy_cost").over(w_kwh_tier_zone), 2).alias("kwh_tier_zone_avg_energy_cost"),
        F.size(F.collect_set("quality").over(w_kwh_tier_zone)).alias("distinct_quality_in_kwh_tier_zone"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "kwh_tier_zone_avg_energy_cost",
        "energy_cost",
        "kwh",
    )
    .unpivot(
        ids=["id"],
        values=["kwh_tier_zone_avg_energy_cost", "energy_cost", "kwh"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
