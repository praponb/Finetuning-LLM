from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_tariff = Window.partitionBy("zone", "tariff")
w_tariff_by_installed_kw_full = Window.partitionBy("tariff").orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_by_installed_kw = Window.partitionBy("tariff").orderBy("installed_kw", "id")
w_all_by_installed_kw_prior_1 = Window.orderBy(F.col("installed_kw").desc(), "id").rowsBetween(-1, -1)

filtered = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.count(F.lit(1)).over(w_zone_tariff).alias("rows_in_zone_tariff"),
        F.first("commissioned_on").over(w_tariff_by_installed_kw_full).alias("first_commissioned_on"),
        F.lag("commissioned_on", 3).over(w_tariff_by_installed_kw).alias("prev3_commissioned_on"),
        F.max("installed_kw").over(w_all_by_installed_kw_prior_1).alias("installed_kw_max_1_prior"),
        F.min("installed_kw").over(w_zone_tariff).alias("zone_tariff_min_installed_kw"),
    )
)

derived = (
    sequenced
    .filter(F.col("prev3_commissioned_on") >= F.lit("2024-08-24").cast("date"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        "first_commissioned_on",
        "prev3_commissioned_on",
        "installed_kw_max_1_prior",
        "zone_tariff_min_installed_kw",
        F.concat_ws(" / ", F.col("zone"), F.col("site_name")).alias("zone_site_name_label"),
        F.concat(F.col("zone"), F.lit("#"), F.col("rows_in_zone_tariff").cast("string")).alias("zone_ref"),
        F.when(F.col("zone").isin("Z2", "Z4"), "group_a").when(F.col("zone") == "Z3", "group_b").otherwise("other").alias("zone_group"),
    )
)

result = (
    derived
    .rollup("zone", "zone_group", "tariff")
    .agg(
        F.sum("installed_kw").alias("total_installed_kw"),
        F.max("first_commissioned_on").alias("last_first_commissioned_on"),
    )
)
