from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff = Window.partitionBy("site_tariff")
w_site_id_by_site_commissioned = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on").asc_nulls_last())
w_all_by_site_commissioned_on_2 = Window.orderBy(F.col("site_commissioned_on").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_quality = Window.partitionBy("quality")
w_site_id_by_site_commissioned_2 = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").desc(), "id")
w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").asc_nulls_last(), "id")
w_site_id_by_site_commissioned_3 = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(-3, Window.currentRow)
w_site_commissioned_on_rank_re = Window.partitionBy("site_commissioned_on_rank", "read_at_quartile").orderBy(F.col("voltage").desc())
w_site_commissioned_on_rank_re_2 = Window.partitionBy("site_commissioned_on_rank", "read_at_quartile").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_commissioned_on_rank_qu = Window.partitionBy("site_commissioned_on_rank", "quality")
w_site_commissioned_on_rank_re_3 = Window.partitionBy("site_commissioned_on_rank", "read_at_quartile").orderBy(F.col("voltage").desc(), "id").rowsBetween(-2, -1)
w_quality_site_commissioned_on = Window.partitionBy("quality", F.month("site_commissioned_on")).orderBy("running_read_at_year", "id")
w_site_commissioned_on_rank_re_4 = Window.partitionBy("site_commissioned_on_rank", "read_at_quartile").orderBy(F.col("voltage").desc(), "id")
w_site_commissioned_on_rank_re_5 = Window.partitionBy("site_commissioned_on_rank", "read_at_quartile").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_meter_readings
    .filter((F.col("quality") == "OK") | (F.col("kwh") <= 90.6))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.year(F.col("read_at")).alias("read_at_year"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

with_parent_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.year(F.col("si.commissioned_on")) == 2025)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_year",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

windowed = (
    with_parent_site
    .select(
        "*",
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_tariff), 2).alias("voltage_vs_site_tariff_avg"),
        F.sum("read_at_year").over(w_site_id_by_site_commissioned).alias("running_read_at_year"),
        F.rank().over(w_all_by_site_commissioned_on).alias("site_commissioned_on_rank"),
        F.max("voltage").over(w_all_by_site_commissioned_on_2).alias("voltage_max_4"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality), F.lit(0)), 4).alias("voltage_share_of_quality"),
        F.ntile(3).over(w_site_id_by_site_commissioned_2).alias("site_commissioned_on_tercile"),
        F.lead("site_commissioned_on", 2).over(w_site_id_quality_by_read_at).alias("next2_site_commissioned_on"),
        F.max("voltage").over(w_site_id_by_site_commissioned_3).alias("voltage_max_4_2"),
        F.ntile(4).over(w_site_id_quality_by_read_at).alias("read_at_quartile"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.round(F.cume_dist().over(w_site_commissioned_on_rank_re), 3).alias("voltage_cume_dist"),
        F.first("site_commissioned_on_tercile").over(w_site_commissioned_on_rank_re_2).alias("first_site_commissioned_on_tercile"),
        F.size(F.collect_set("site_tariff").over(w_site_commissioned_on_rank_qu)).alias("distinct_site_tariff_in_site_commissioned_on_ran"),
        F.round(F.avg("read_at_year").over(w_site_commissioned_on_rank_re_3), 2).alias("read_at_year_avg_2_prior"),
        F.ntile(3).over(w_quality_site_commissioned_on).alias("running_read_at_year_tercile"),
        F.round(F.percent_rank().over(w_quality_site_commissioned_on), 3).alias("running_read_at_year_percent_rank"),
        F.row_number().over(w_site_commissioned_on_rank_re_4).alias("voltage_seq"),
        F.last("site_commissioned_on_rank", ignorenulls=True).over(w_site_commissioned_on_rank_re_5).alias("last_known_site_commissioned_on_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("first_site_commissioned_on_tercile") < 7)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "read_at_year",
        "site_tariff",
        "site_commissioned_on",
        "voltage_vs_site_tariff_avg",
        "running_read_at_year",
        "site_commissioned_on_rank",
        "voltage_max_4",
        "site_commissioned_on_tercile",
        "next2_site_commissioned_on",
        "read_at_quartile",
        "voltage_cume_dist",
        "first_site_commissioned_on_tercile",
        "distinct_site_tariff_in_site_commissioned_on_ran",
        "read_at_year_avg_2_prior",
        "running_read_at_year_percent_rank",
        "voltage_seq",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
    .orderBy(F.col("site_id").desc())
)
