from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_read_at_month_by_q = Window.partitionBy("site_zone", F.date_trunc("month", "read_at")).orderBy(F.col("quality_len").asc_nulls_last())
w_quality_len = Window.partitionBy("quality_len")
w_quality_len_by_kwh_ties = Window.partitionBy("quality_len").orderBy(F.col("kwh").desc())
w_site_zone = Window.partitionBy("site_zone")
w_site_id_quality_len_by_kwh = Window.partitionBy("site_id", "quality_len").orderBy(F.col("kwh").desc(), "id")
w_site_id_quality_len_by_kwh_t = Window.partitionBy("site_id", "quality_len").orderBy(F.col("kwh").desc())
w_site_zone_by_quality_len_mov = Window.partitionBy("site_zone").orderBy("quality_len", "id").rowsBetween(-3, Window.currentRow)
w_all_by_quality_len_cume_dist = Window.orderBy(F.col("quality_len_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_quality_len_runni = Window.partitionBy("site_id").orderBy(F.col("quality_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_len_by_kwh_dense_ran = Window.partitionBy("quality_len").orderBy("kwh_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_zone_kwh_quartile = Window.partitionBy("site_zone", "kwh_quartile")
w_kwh_dense_rank = Window.partitionBy("kwh_dense_rank")

scoped = (
    df_meter_readings
    .filter((F.col("voltage") > 222.6) & (F.to_date(F.col("read_at")) < F.lit("2025-02-06").cast("date")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

with_details_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality_len",
        F.col("si.zone").alias("site_zone"),
    )
)

windowed = (
    with_details_site
    .select(
        "id",
        "site_id",
        "kwh",
        "quality_len",
        "site_zone",
        F.round(F.cume_dist().over(w_site_zone_read_at_month_by_q), 3).alias("quality_len_cume_dist"),
        F.min("quality_len").over(w_quality_len).alias("quality_len_min_quality_len"),
        F.round(F.percent_rank().over(w_quality_len_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.sum("quality_len").over(w_site_zone).alias("site_zone_sum_quality_len"),
        F.sum("kwh").over(w_quality_len).alias("quality_len_sum_kwh"),
        F.rank().over(w_site_id_quality_len_by_kwh).alias("kwh_rank"),
        F.dense_rank().over(w_site_id_quality_len_by_kwh_t).alias("kwh_dense_rank"),
        F.ntile(4).over(w_site_id_quality_len_by_kwh).alias("kwh_quartile"),
        F.round(F.avg("kwh").over(w_site_zone), 2).alias("site_zone_avg_kwh"),
        F.sum("quality_len").over(w_site_zone_by_quality_len_mov).alias("quality_len_sum_4"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "site_id",
        "quality_len",
        "quality_len_cume_dist",
        "quality_len_min_quality_len",
        "kwh_percent_rank",
        "site_zone_sum_quality_len",
        "quality_len_sum_kwh",
        "kwh_rank",
        "site_zone_avg_kwh",
        "quality_len_sum_4",
        F.min("kwh_quartile").over(w_all_by_quality_len_cume_dist).alias("min_kwh_quartile_so_far"),
        F.first("kwh", ignorenulls=True).over(w_site_id_by_quality_len_runni).alias("first_known_kwh"),
        F.last("kwh").over(w_quality_len_by_kwh_dense_ran).alias("last_kwh"),
        F.size(F.collect_set("site_id").over(w_site_zone_kwh_quartile)).alias("distinct_site_id_in_site_zone_kwh_quartile"),
        F.size(F.collect_set("site_id").over(w_kwh_dense_rank)).alias("distinct_site_id_in_kwh_dense_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("quality_len") <= 12)
    .select(
        "id",
        "site_id",
        "quality_len",
        "quality_len_min_quality_len",
        "site_zone_sum_quality_len",
        "quality_len_sum_kwh",
        "site_zone_avg_kwh",
        "min_kwh_quartile_so_far",
        "first_known_kwh",
        "last_kwh",
        "distinct_site_id_in_site_zone_kwh_quartile",
        "distinct_site_id_in_kwh_dense_rank",
        F.round(F.col("quality_len_sum_kwh") * 0.25, 2).alias("quality_len_sum_kwh_adj"),
    )
    .orderBy(F.col("site_id").desc(), "id")
    .limit(10)
)
