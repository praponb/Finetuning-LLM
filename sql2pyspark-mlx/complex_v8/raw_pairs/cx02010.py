from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter(F.upper(F.trim(F.col("site_name"))).like("S%"))
    .select(
        F.col("id").alias("key_id"),
        F.col("installed_kw").alias("amount"),
        F.lit("site").alias("record_type"),
    )
)

tariff_side = (
    df_tariff_windows
    .filter(F.col("tariff") == "FLAT")
    .select("id", "rate", F.lit("tariff").alias("record_type"))
)

result = site_side.union(tariff_side)
