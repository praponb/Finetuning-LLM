from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_commissioned_on_pr = Window.partitionBy("tariff").orderBy("commissioned_on", "id").rowsBetween(-1, -1)
w_zone = Window.partitionBy("zone")
w_zone_commissioned_on_year_by = Window.partitionBy("zone", F.year("commissioned_on")).orderBy("commissioned_on", "id")
w_all_by_installed_kw_bucket = Window.orderBy(F.col("installed_kw_bucket").desc(), "id")
w_commissioned_on_year_by_inst = Window.partitionBy(F.year("commissioned_on")).orderBy("installed_kw_bucket", "id")
w_zone_commissioned_on_year_by_2 = Window.partitionBy("zone", F.year("commissioned_on")).orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone_group_commissioned_on_m = Window.partitionBy("zone_group", F.date_trunc("month", "commissioned_on")).orderBy(F.col("commissioned_on").desc(), "id")

valid = (
    df_sites
    .filter((F.col("commissioned_on").between(F.lit("2024-04-23").cast("date"), F.lit("2025-11-28").cast("date"))) & (F.upper(F.trim(F.col("site_name"))).like("H%")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
        F.when(F.col("zone").isin("Z3", "Z4"), "group_a").when(F.col("zone") == "Z2", "group_b").otherwise("other").alias("zone_group"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw_bucket",
        "zone_group",
        F.max("installed_kw_bucket").over(w_tariff_by_commissioned_on_pr).alias("installed_kw_bucket_max_1_prior"),
        F.count(F.lit(1)).over(w_zone).alias("rows_in_zone"),
        F.round(F.col("installed_kw_bucket") / F.nullif(F.sum("installed_kw_bucket").over(w_zone), F.lit(0)), 4).alias("installed_kw_bucket_share_of_zone"),
        F.datediff(F.col("commissioned_on"), F.lag("commissioned_on").over(w_zone_commissioned_on_year_by)).alias("days_since_prev_commissioned_on"),
        F.lead("installed_kw_bucket").over(w_all_by_installed_kw_bucket).alias("next_installed_kw_bucket"),
        F.lag("commissioned_on").over(w_commissioned_on_year_by_inst).alias("prev_commissioned_on"),
        F.last("installed_kw_bucket").over(w_zone_commissioned_on_year_by_2).alias("last_installed_kw_bucket"),
        F.round(F.percent_rank().over(w_all_by_installed_kw_bucket), 3).alias("installed_kw_bucket_percent_rank"),
        F.lag("commissioned_on", 2).over(w_zone_group_commissioned_on_m).alias("prev2_commissioned_on"),
        F.lag("installed_kw_bucket").over(w_zone_commissioned_on_year_by).alias("prev_installed_kw_bucket"),
    )
)

result = (
    metrics
    .filter(F.col("zone_group") != "group_b")
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw_bucket",
        "installed_kw_bucket_max_1_prior",
        "rows_in_zone",
        "days_since_prev_commissioned_on",
        "prev_commissioned_on",
        "prev2_commissioned_on",
        "prev_installed_kw_bucket",
    )
    .orderBy("id")
)
