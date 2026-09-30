from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("zone").orderBy("installed_kw", "id")

base_sites = (
    df_sites
    .filter(F.col("tariff") != "FLAT")
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        F.when(F.col("installed_kw") >= 386.6, "A").when(F.col("installed_kw") >= 93.3, "B").otherwise("C").alias("installed_kw_tier"),
        F.substring(F.trim(F.col("zone")), 1, 2).alias("zone_prefix"),
        F.concat_ws(" / ", F.col("site_name"), F.col("zone")).alias("site_name_zone_label"),
    )
)

latest_per_key = (
    base_sites
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

without_matches = (
    latest_per_key
    .crossJoin(latest_per_key.agg(F.avg("installed_kw").alias("overall_avg_installed_kw")))
    .filter(F.col("installed_kw") > F.col("overall_avg_installed_kw"))
    .drop("overall_avg_installed_kw")
)

annotated = (
    without_matches
    .select(
        "*",
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
        F.coalesce(F.col("installed_kw"), F.lit(0.0)).alias("installed_kw_filled"),
    )
)

result = (
    annotated
    .select(
        "id",
        "installed_kw_tier",
        "zone_prefix",
        "site_name_zone_label",
        "zone_or_unknown",
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
        F.when(F.col("installed_kw_filled") >= 351.5, "large").when(F.col("installed_kw_filled") >= 203.0, "medium").otherwise("small").alias("installed_kw_filled_tier"),
    )
    .orderBy("zone_prefix", "id")
    .limit(20)
)
