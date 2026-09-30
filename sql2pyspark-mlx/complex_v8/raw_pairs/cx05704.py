from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("tariff", "installed_kw_tier").orderBy("installed_kw", "id")

base = (
    df_sites
    .filter((F.col("tariff").isNotNull()) & (F.lower(F.col("site_name")).like("%m %")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.when(F.col("installed_kw") >= 284.0, "A").when(F.col("installed_kw") >= 96.9, "B").otherwise("C").alias("installed_kw_tier"),
        F.add_months(F.col("commissioned_on"), 1).alias("commissioned_on_plus_1m"),
    )
)

tariff_stats = (
    base
    .groupBy("tariff")
    .agg(F.avg("installed_kw").alias("avg_installed_kw"))
)

outliers = (
    base.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.installed_kw") > F.col("g.avg_installed_kw"))
    .select(
        "s.*",
        "g.avg_installed_kw",
        F.round(F.col("s.installed_kw") - F.col("g.avg_installed_kw"), 2).alias("installed_kw_minus_avg"),
    )
)

latest_per_key = (
    outliers
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        "site_name",
        "commissioned_on",
        "installed_kw_tier",
        "commissioned_on_plus_1m",
        "installed_kw_minus_avg",
        F.dayofweek(F.col("commissioned_on")).alias("commissioned_on_dayofweek"),
    )
    .orderBy(F.col("site_name").desc())
)
