from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_hour_by_read_at_full = Window.partitionBy("read_at_hour").orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_hour_by_read_at = Window.partitionBy("read_at_hour").orderBy(F.col("read_at").asc_nulls_last(), "id")
w_read_at_hour_by_read_at_ties = Window.partitionBy("read_at_hour").orderBy(F.col("read_at").asc_nulls_last())
w_site_tariff_read_at_hour = Window.partitionBy("site_tariff", "read_at_hour")
w_read_at_year_by_read_at_movi = Window.partitionBy(F.year("read_at")).orderBy(F.col("read_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_read_at_hour_by_read_at_movi = Window.partitionBy("read_at_hour").orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_read_at_month_by_read_at_hou = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("read_at_hour").asc_nulls_last(), "id")
w_read_at_hour_by_read_at_movi_2 = Window.partitionBy("read_at_hour").orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_quality_distinct_site_id_in = Window.partitionBy("quality", "distinct_site_id_in_site_tariff_read_at_hour").orderBy("read_at_hour_quintile", "id")
w_site_tariff_by_read_at_perce = Window.partitionBy("site_tariff").orderBy(F.col("read_at_percent_rank").desc(), "id")
w_quality_distinct_site_id_in_2 = Window.partitionBy("quality", "distinct_site_id_in_site_tariff_read_at_hour").orderBy("read_at_hour_quintile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id_by_read_at_p = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at_percent_rank").desc(), "id")

valid_readings = (
    df_meter_readings
    .filter(F.col("quality") != "EST")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.hour(F.col("read_at")).alias("read_at_hour"),
    )
)

with_details_site = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(~F.col("si.tariff").isin("FLAT", "EV"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.read_at_hour",
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

sequenced = (
    with_details_site
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "read_at_hour",
        "site_tariff",
        "site_name",
        F.last("read_at_hour").over(w_read_at_hour_by_read_at_full).alias("last_read_at_hour"),
        F.round(F.percent_rank().over(w_read_at_hour_by_read_at), 3).alias("read_at_percent_rank"),
        F.round(F.cume_dist().over(w_read_at_hour_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.size(F.collect_set("site_id").over(w_site_tariff_read_at_hour)).alias("distinct_site_id_in_site_tariff_read_at_hour"),
        F.round(F.cume_dist().over(w_read_at_hour_by_read_at), 3).alias("read_at_cume_dist_2"),
        F.round(F.avg("kwh").over(w_read_at_year_by_read_at_movi), 2).alias("kwh_avg_4"),
        F.sum("read_at_hour").over(w_read_at_hour_by_read_at_movi).alias("read_at_hour_sum_4"),
        F.ntile(5).over(w_read_at_month_by_read_at_hou).alias("read_at_hour_quintile"),
        F.lead("read_at_hour").over(w_read_at_hour_by_read_at).alias("next_read_at_hour"),
        F.round(F.avg("kwh").over(w_read_at_hour_by_read_at_movi_2), 2).alias("kwh_avg_2"),
        F.ntile(4).over(w_read_at_hour_by_read_at).alias("read_at_quartile"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "read_at",
        "quality",
        "read_at_hour",
        "last_read_at_hour",
        "read_at_percent_rank",
        "distinct_site_id_in_site_tariff_read_at_hour",
        "read_at_cume_dist_2",
        "read_at_hour_quintile",
        "next_read_at_hour",
        "read_at_quartile",
        F.round(F.percent_rank().over(w_quality_distinct_site_id_in), 3).alias("read_at_hour_quintile_percent_rank"),
        F.round(F.cume_dist().over(w_site_tariff_by_read_at_perce), 3).alias("read_at_percent_rank_cume_dist"),
        F.lag("read_at").over(w_site_tariff_by_read_at_perce).alias("prev_read_at"),
        F.first("kwh_avg_2", ignorenulls=True).over(w_quality_distinct_site_id_in_2).alias("first_known_kwh_avg_2"),
        F.ntile(4).over(w_quality_site_id_by_read_at_p).alias("read_at_percent_rank_quartile"),
    )
)

result = (
    metrics
    .select(
        "id",
        "read_at",
        "quality",
        "last_read_at_hour",
        "read_at_percent_rank",
        "distinct_site_id_in_site_tariff_read_at_hour",
        "read_at_cume_dist_2",
        "read_at_hour_quintile",
        "next_read_at_hour",
        "read_at_quartile",
        "read_at_hour_quintile_percent_rank",
        "read_at_percent_rank_cume_dist",
        "prev_read_at",
        "read_at_percent_rank_quartile",
        (F.floor(F.col("read_at_hour_quintile") / 2) * 2).alias("read_at_hour_quintile_bucket"),
        F.coalesce(F.col("read_at_percent_rank"), F.lit(0.0)).alias("read_at_percent_rank_filled"),
    )
    .orderBy(F.col("read_at_percent_rank_filled").desc(), "id")
    .limit(5)
)
