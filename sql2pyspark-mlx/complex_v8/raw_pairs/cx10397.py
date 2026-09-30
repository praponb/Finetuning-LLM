from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_year_zone_by = Window.partitionBy(F.year("commissioned_on"), "zone").orderBy("commissioned_on", "id")
w_commissioned_on_month_tariff = Window.partitionBy(F.date_trunc("month", "commissioned_on"), "tariff").orderBy(F.col("commissioned_on").desc(), "id")
w_zone_by_commissioned_on = Window.partitionBy("zone").orderBy("commissioned_on", "id")
w_zone_by_commissioned_on_ties = Window.partitionBy("zone").orderBy("commissioned_on")
w_tariff_by_commissioned_on = Window.partitionBy("tariff").orderBy(F.col("commissioned_on").desc(), "id")
w_tariff_by_commissioned_on_2 = Window.partitionBy("tariff").orderBy("commissioned_on", "id")
w_zone_by_commissioned_on_runn = Window.partitionBy("zone").orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_commissioned_on_year_zone_by_2 = Window.partitionBy(F.year("commissioned_on"), "zone").orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_by_commissioned_on_fu = Window.partitionBy("tariff").orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_sites
    .filter(F.year(F.col("commissioned_on")) == 2024)
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        F.substring(F.trim(F.col("site_name")), 1, 4).alias("site_name_prefix"),
    )
)

windowed = (
    prepared
    .select(
        "id",
        "zone",
        "tariff",
        "site_name_prefix",
        F.lag("commissioned_on", 2).over(w_commissioned_on_year_zone_by).alias("prev2_commissioned_on"),
        F.row_number().over(w_commissioned_on_month_tariff).alias("rn"),
        F.lead("commissioned_on").over(w_zone_by_commissioned_on).alias("next_commissioned_on"),
        F.round(F.cume_dist().over(w_zone_by_commissioned_on_ties), 3).alias("commissioned_on_cume_dist"),
        F.round(F.percent_rank().over(w_tariff_by_commissioned_on), 3).alias("commissioned_on_percent_rank"),
        F.lead("commissioned_on").over(w_commissioned_on_month_tariff).alias("next_commissioned_on_2"),
        F.ntile(5).over(w_tariff_by_commissioned_on_2).alias("commissioned_on_quintile"),
        F.dense_rank().over(w_tariff_by_commissioned_on).alias("commissioned_on_dense_rank"),
        F.first("commissioned_on", ignorenulls=True).over(w_zone_by_commissioned_on_runn).alias("first_known_commissioned_on"),
        F.ntile(3).over(w_zone_by_commissioned_on).alias("commissioned_on_tercile"),
        F.nth_value("commissioned_on", 2).over(w_commissioned_on_year_zone_by_2).alias("second_commissioned_on"),
        F.ntile(10).over(w_tariff_by_commissioned_on).alias("commissioned_on_decile"),
        F.last("commissioned_on").over(w_tariff_by_commissioned_on_fu).alias("last_commissioned_on"),
        F.dense_rank().over(w_commissioned_on_month_tariff).alias("commissioned_on_dense_rank_2"),
        F.first("commissioned_on").over(w_tariff_by_commissioned_on_fu).alias("first_commissioned_on"),
        F.ntile(4).over(w_commissioned_on_month_tariff).alias("commissioned_on_quartile"),
    )
)

result = (
    windowed
    .select(
        "id",
        "next_commissioned_on_2",
        "commissioned_on_quintile",
        "commissioned_on_dense_rank",
        "first_known_commissioned_on",
        "commissioned_on_tercile",
        "second_commissioned_on",
        "commissioned_on_decile",
        "commissioned_on_dense_rank_2",
        "first_commissioned_on",
        "commissioned_on_quartile",
        F.concat_ws(" / ", F.col("zone"), F.col("site_name_prefix")).alias("zone_site_name_prefix_label"),
        F.round(F.col("rn") / F.nullif(F.col("commissioned_on_decile"), F.lit(0)), 3).alias("rn_per_commissioned_on_decile"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
