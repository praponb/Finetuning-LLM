from pyspark.sql import functions as F

recent_sites = (
    df_sites
    .filter((F.col("commissioned_on") >= F.lit("2024-08-24").cast("date")) & (F.col("tariff") == "TOU"))
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

tagged = (
    recent_sites
    .filter(F.col("commissioned_on").between(F.lit("2024-08-24").cast("date"), F.lit("2025-12-03").cast("date")))
    .select(
        "*",
        F.when(F.col("installed_kw") >= 284.0, "A").when(F.col("installed_kw") >= 96.9, "B").otherwise("C").alias("installed_kw_tier"),
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)

scored = (
    tagged
    .select(
        "*",
        F.concat_ws("|", F.col("installed_kw_tier"), F.col("zone")).alias("installed_kw_tier_zone_label"),
        F.abs(F.col("installed_kw_bucket") - 154).alias("installed_kw_bucket_distance"),
        (F.col("installed_kw_bucket") + F.col("installed_kw")).alias("installed_kw_bucket_plus_installed_kw"),
    )
)

result = (
    scored
    .cube("zone", "installed_kw_tier")
    .agg(F.count("*").alias("cnt"))
    .select(
        F.coalesce(F.col("zone"), F.lit("ALL")).alias("zone"),
        F.coalesce(F.col("installed_kw_tier"), F.lit("ALL")).alias("installed_kw_tier"),
        "cnt",
    )
    .orderBy(
        F.col("zone").asc_nulls_last(),
        F.col("installed_kw_tier").asc_nulls_last(),
    )
)
