from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_quality_site_id_by_read_at_t = Window.partitionBy("quality", "site_id").orderBy("read_at")
w_quality_site_id_by_site_inst = Window.partitionBy("quality", "site_id").orderBy("site_installed_kw", "id")
w_site_id_quality_by_kwh_ties = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").asc_nulls_last())
w_quality_site_id_by_site_inst_2 = Window.partitionBy("quality", "site_id").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality_by_kwh = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_max_kwh_rank_by_n_rows = Window.partitionBy("max_kwh_rank").orderBy("n_rows", "read_at_dense_rank")
w_read_at_dense_rank = Window.partitionBy("read_at_dense_rank")

filtered = (
    df_meter_readings
    .filter(F.col("site_id") <= 5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw").between(96.9, 373.3))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

windowed = (
    with_parent
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_id), F.lit(0)), 4).alias("site_installed_kw_share_of_site_id"),
        F.dense_rank().over(w_quality_site_id_by_read_at_t).alias("read_at_dense_rank"),
        (F.col("voltage") - F.lag("voltage").over(w_quality_site_id_by_site_inst)).alias("voltage_change"),
        F.round(F.avg("kwh").over(w_site_id), 2).alias("site_id_avg_kwh"),
        F.rank().over(w_site_id_quality_by_kwh_ties).alias("kwh_rank"),
        F.first("kwh").over(w_quality_site_id_by_site_inst_2).alias("first_kwh"),
        (F.col("site_installed_kw") - F.lag("site_installed_kw").over(w_quality_site_id_by_site_inst)).alias("site_installed_kw_change"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_site_id_quality_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
    )
)

first_rows = windowed.filter(F.col("kwh_rank") <= 3)

stats = (
    first_rows
    .filter(F.col("quality") != "BAD")
    .groupBy("read_at_dense_rank")
    .agg(
        F.count("*").alias("n_rows"),
        F.min("read_at").alias("first_read_at"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.sum("kwh_rank").alias("total_kwh_rank"),
        F.sum(F.when(F.col("quality") == "OK", F.col("site_installed_kw_change")).otherwise(0)).alias("site_installed_kw_change_quality_ok"),
        F.max("kwh_rank").alias("max_kwh_rank"),
    )
)

with_windows = (
    stats
    .withColumn("n_rows_quintile", F.ntile(5).over(w_max_kwh_rank_by_n_rows))
    .withColumn("distinct_qualitys_share_of_read_at_dense_rank", F.round(F.col("distinct_qualitys") / F.nullif(F.sum("distinct_qualitys").over(w_read_at_dense_rank), F.lit(0)), 4))
)

result = (
    with_windows
    .select(
        "read_at_dense_rank",
        "first_read_at",
        "distinct_qualitys",
        "site_installed_kw_change_quality_ok",
        "n_rows_quintile",
        "distinct_qualitys_share_of_read_at_dense_rank",
        F.coalesce(F.col("total_kwh_rank"), F.lit(0)).alias("total_kwh_rank_filled"),
        F.to_date(F.col("first_read_at")).alias("first_read_at_date"),
    )
)
