from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_installed_kw_bucket = Window.orderBy(F.col("installed_kw_bucket").desc(), "id")
w_zone = Window.partitionBy("zone")
w_dedup = Window.partitionBy("zone").orderBy(F.col("installed_kw_bucket_change").desc(), F.col("id").desc())
w_zone_by_installed_kw_bucket = Window.partitionBy("zone").orderBy("installed_kw_bucket_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_zone_by_installed_kw_bucket_2 = Window.partitionBy("zone").orderBy("installed_kw_bucket_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone_by_installed_kw_bucket_3 = Window.partitionBy("zone").orderBy("installed_kw_bucket_percent_rank", "id").rowsBetween(-6, Window.currentRow)

cleaned_sites = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)

ranked = (
    cleaned_sites
    .select(
        "*",
        (F.col("installed_kw_bucket") - F.lag("installed_kw_bucket").over(w_all_by_installed_kw_bucket)).alias("installed_kw_bucket_change"),
        F.round(F.percent_rank().over(w_all_by_installed_kw_bucket), 3).alias("installed_kw_bucket_percent_rank"),
        F.round(F.col("installed_kw_bucket") / F.nullif(F.sum("installed_kw_bucket").over(w_zone), F.lit(0)), 4).alias("installed_kw_bucket_share_of_zone"),
    )
)

deduped = (
    ranked
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    deduped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.kwh") > 48.2) & (F.col("m.quality") == "OK"), "left_semi")
    .filter(F.col("installed_kw_bucket_percent_rank") <= 0.8)
)

with_trends = (
    filtered
    .select(
        "*",
        F.last("installed_kw_bucket_change", ignorenulls=True).over(w_zone_by_installed_kw_bucket).alias("last_known_installed_kw_bucket_change"),
        F.first("installed_kw_bucket_change").over(w_zone_by_installed_kw_bucket_2).alias("first_installed_kw_bucket_change"),
        F.round(F.avg("installed_kw").over(w_zone_by_installed_kw_bucket_3), 2).alias("installed_kw_avg_7"),
        F.sum("installed_kw_bucket_change").over(w_zone).alias("zone_sum_installed_kw_bucket_change"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "installed_kw_bucket_change",
        "last_known_installed_kw_bucket_change",
        "installed_kw_avg_7",
        "zone_sum_installed_kw_bucket_change",
        F.concat(F.col("zone"), F.lit("#"), F.col("installed_kw_bucket").cast("string")).alias("zone_ref"),
    )
    .orderBy(
        F.col("zone_sum_installed_kw_bucket_change").desc_nulls_first(),
        F.col("last_known_installed_kw_bucket_change").desc(),
    )
)
