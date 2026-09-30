from pyspark.sql import functions as F

filtered_tariffs = (
    df_tariff_windows
    .filter((F.col("tariff").isin("EV", "TOU")) & (F.col("rate") < 5.5))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
    )
)

tariff_side = filtered_tariffs.select("id")

site_rows = (
    df_sites
    .filter(F.col("installed_kw") > 141.6)
    .select("id")
)

result = (
    tariff_side
    .subtract(site_rows)
    .orderBy("id")
)
