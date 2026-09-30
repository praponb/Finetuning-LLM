from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_tariff_by_installed_kw = Window.partitionBy("zone", "tariff").orderBy(F.col("installed_kw_adj_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone_group_tariff = Window.partitionBy("zone_group", "tariff")
w_all_by_installed_kw_adj_fill = Window.orderBy(F.col("installed_kw_adj_filled").desc(), "id")
w_zone_zone_group_by_installed = Window.partitionBy("zone", "zone_group").orderBy("installed_kw_adj_filled", "id")

valid = (
    df_sites
    .filter((F.col("zone") == "Z4") & (F.length(F.trim(F.col("site_name"))) >= 6))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        F.round(F.col("installed_kw") * 1.07, 2).alias("installed_kw_adj"),
        F.when(F.col("zone").isin("Z2", "Z3"), "group_a").when(F.col("zone") == "Z4", "group_b").otherwise("other").alias("zone_group"),
        F.when(F.col("zone").isin("Z4", "Z2"), "group_a").when(F.col("zone") == "Z3", "group_b").otherwise("other").alias("zone_group_2"),
    )
)

scored = (
    valid
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "zone_group",
        F.coalesce(F.col("installed_kw_adj"), F.lit(0.0)).alias("installed_kw_adj_filled"),
    )
)

ranked = (
    scored
    .select(
        "*",
        F.nth_value("installed_kw_adj_filled", 2).over(w_zone_tariff_by_installed_kw).alias("second_installed_kw_adj_filled"),
        F.round(F.avg("installed_kw_adj_filled").over(w_zone_group_tariff), 2).alias("zone_group_tariff_avg_installed_kw_adj_filled"),
        F.row_number().over(w_all_by_installed_kw_adj_fill).alias("rn"),
        F.max("installed_kw_adj_filled").over(w_zone_group_tariff).alias("zone_group_tariff_max_installed_kw_adj_filled"),
        F.ntile(4).over(w_zone_zone_group_by_installed).alias("installed_kw_adj_filled_quartile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_name",
        "zone",
        "zone_group",
        "installed_kw_adj_filled",
        "second_installed_kw_adj_filled",
        "zone_group_tariff_avg_installed_kw_adj_filled",
        "zone_group_tariff_max_installed_kw_adj_filled",
    )
    .orderBy(
        F.col("zone_group_tariff_avg_installed_kw_adj_filled").asc(),
        "zone_group_tariff_max_installed_kw_adj_filled",
    )
)
