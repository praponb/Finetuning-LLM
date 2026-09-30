from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_len_zone_by_avg_tarif = Window.partitionBy("tariff_len", "zone").orderBy(F.col("avg_tariff_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone_by_installed_kw_ties = Window.partitionBy("zone").orderBy(F.col("installed_kw").desc())
w_zone_by_installed_kw = Window.partitionBy("zone").orderBy(F.col("installed_kw").desc(), "id")
w_tariff_len_zone_by_avg_tarif_2 = Window.partitionBy("tariff_len", "zone").orderBy(F.col("avg_tariff_len").desc())
w_commissioned_on_year_tariff = Window.partitionBy(F.year("commissioned_on"), "tariff_len")

recent = (
    df_sites
    .filter((F.col("zone") == "Z2") & (F.col("tariff") == "EV"))
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
    )
)

zone_stats = (
    recent
    .groupBy("zone")
    .agg(F.avg("tariff_len").alias("avg_tariff_len"))
)

above_average = (
    recent.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.tariff_len") > F.col("g.avg_tariff_len"))
    .select(
        "s.*",
        "g.avg_tariff_len",
        F.round(F.col("s.tariff_len") - F.col("g.avg_tariff_len"), 2).alias("tariff_len_minus_avg"),
    )
)

scored = (
    above_average
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
        F.abs(F.col("tariff_len") - 10).alias("tariff_len_distance"),
    )
)

sequenced = (
    scored
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
        "tariff_len",
        "avg_tariff_len",
        "tariff_len_minus_avg",
        "tariff_len_distance",
        F.first("installed_kw").over(w_tariff_len_zone_by_avg_tarif).alias("first_installed_kw"),
        F.dense_rank().over(w_zone_by_installed_kw_ties).alias("installed_kw_dense_rank"),
        F.lag("installed_kw").over(w_zone_by_installed_kw).alias("prev_installed_kw"),
        F.rank().over(w_tariff_len_zone_by_avg_tarif_2).alias("avg_tariff_len_rank"),
        F.max("tariff_len_distance").over(w_commissioned_on_year_tariff).alias("commissioned_on_year_tariff_len_max_tariff_len_d"),
        F.rank().over(w_zone_by_installed_kw).alias("installed_kw_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
        "tariff_len",
        "avg_tariff_len",
        "tariff_len_minus_avg",
        "tariff_len_distance",
        "installed_kw_dense_rank",
        "prev_installed_kw",
        "avg_tariff_len_rank",
        "installed_kw_rank",
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
    )
    .orderBy(F.col("tariff_len").desc(), F.col("installed_kw_dense_rank").asc(), "id")
    .limit(20)
)
