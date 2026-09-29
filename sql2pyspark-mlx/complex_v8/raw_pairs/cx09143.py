from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.col("tariff").isin("FLAT", "TOU")) & (F.col("installed_kw") < 386.6))
    .select("id")
)

tariff_side = (
    df_tariff_windows
    .filter((F.month(F.col("valid_to")).isin(1, 2)) & (F.col("tariff") != "FLAT"))
    .select("id")
)

result = site_side.exceptAll(tariff_side)
