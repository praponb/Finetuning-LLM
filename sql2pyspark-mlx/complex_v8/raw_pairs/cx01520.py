from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_installed_kw_prior_1 = Window.orderBy("installed_kw", "id").rowsBetween(-1, -1)
w_all_by_installed_kw = Window.orderBy("installed_kw", "id")
w_all_by_installed_kw_moving_6 = Window.orderBy("installed_kw", "id").rowsBetween(-6, Window.currentRow)
w_latest = Window.partitionBy("zone").orderBy(F.col("installed_kw").desc(), F.col("id").desc())

recent_sites = (
    df_sites
    .filter(F.col("installed_kw").between(96.9, 446.9))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
        F.concat_ws("-", F.col("site_name"), F.col("zone")).alias("site_name_zone_label"),
        F.date_add(F.col("commissioned_on"), 7).alias("commissioned_on_plus_7d"),
    )
)

qualified = (
    recent_sites.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.kwh") > 45.0) & (F.hour(F.col("m.read_at")).between(10, 14)), "left_semi")
)

sequenced = (
    qualified
    .select(
        "*",
        F.round(F.avg("installed_kw").over(w_all_by_installed_kw_prior_1), 2).alias("installed_kw_avg_1_prior"),
        F.dense_rank().over(w_all_by_installed_kw).alias("installed_kw_dense_rank"),
        F.round(F.avg("installed_kw").over(w_all_by_installed_kw_moving_6), 2).alias("installed_kw_avg_7"),
        F.lag("installed_kw", 3, 0).over(w_all_by_installed_kw).alias("prev3_installed_kw"),
    )
)

latest_per_key = (
    sequenced
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_stats = (
    latest_per_key.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.first_reading_read_at",
    )
)

result = (
    with_stats
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
        "site_name_zone_label",
        "commissioned_on_plus_7d",
        "installed_kw_avg_1_prior",
        "installed_kw_dense_rank",
        "reading_count",
        "first_reading_read_at",
    )
    .orderBy(
        F.col("reading_count").desc(),
        F.col("commissioned_on").asc_nulls_last(),
    )
)
