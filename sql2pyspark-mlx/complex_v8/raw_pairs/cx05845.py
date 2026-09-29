from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("site_installed_kw").desc(), F.col("id").desc())

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

joined = (
    in_window.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.energy_cost",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

scored = (
    joined
    .filter(F.col("energy_cost") <= 630.0)
    .select(
        "id",
        "site_id",
        "quality",
        "energy_cost",
        "site_installed_kw",
        (F.floor(F.col("energy_cost") / 250) * 250).alias("energy_cost_bucket"),
        F.add_months(F.col("site_commissioned_on"), 6).alias("site_commissioned_on_plus_6m"),
    )
)

latest_per_key = (
    scored
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        F.col("site_installed_kw").alias("m_site_installed_kw"),
        F.col("energy_cost").alias("energy_cost_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_site_installed_kw", "energy_cost_v"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
