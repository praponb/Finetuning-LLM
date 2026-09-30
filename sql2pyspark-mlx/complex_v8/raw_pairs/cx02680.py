from pyspark.sql import functions as F

valid_tariffs = (
    df_tariff_windows
    .filter(F.col("valid_to") >= F.lit("2025-02-15 04:00:00").cast("timestamp"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.when(F.col("tariff").isin("FLAT", "EV"), "group_a").when(F.col("tariff") == "TOU", "group_b").otherwise("other").alias("tariff_group"),
    )
)

tariff_side = (
    df_tariff_windows
    .filter(F.col("tariff") == "EV")
    .select("id", "rate")
)

site_rows = df_sites.select("id", "installed_kw")

result = (
    tariff_side
    .union(site_rows)
    .distinct()
    .orderBy("rate")
)
