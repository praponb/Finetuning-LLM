from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_quality_by_read_at = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id_by_site_installed_kw = Window.partitionBy("site_id").orderBy("site_installed_kw")
w_read_at_to_date_by_read_at = Window.partitionBy(F.to_date("read_at")).orderBy("read_at", "id")
w_quality_by_read_at_moving_1 = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(-1, Window.currentRow)
w_site_id_read_at_to_date_by_s = Window.partitionBy("site_id", F.to_date("read_at")).orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_to_date_by_read_at_t = Window.partitionBy(F.to_date("read_at")).orderBy("read_at")
w_quality_read_at_dense_rank_b = Window.partitionBy("quality", "read_at_dense_rank").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_read_at_dense_rank_b_2 = Window.partitionBy("quality", "read_at_dense_rank").orderBy("site_installed_kw", "id")
w_read_at_quartile_quality_by = Window.partitionBy("read_at_quartile", "quality").orderBy(F.col("site_installed_kw_share_of_quality_site_id").asc_nulls_last(), "id")
w_quality_read_at_dense_rank_b_3 = Window.partitionBy("quality", "read_at_dense_rank").orderBy("site_installed_kw", "id").rowsBetween(-3, -1)
w_read_at_quartile_quality = Window.partitionBy("read_at_quartile", "quality")

prepared_readings = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")) & (F.col("site_id") >= 2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

combined_details_site = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw").between(96.9, 386.6))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

windowed = (
    combined_details_site
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "site_installed_kw",
        F.max("site_installed_kw").over(w_quality).alias("quality_max_site_installed_kw"),
        F.ntile(4).over(w_quality_by_read_at).alias("read_at_quartile"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.dense_rank().over(w_site_id_by_site_installed_kw).alias("site_installed_kw_dense_rank"),
        (F.col("site_installed_kw") - F.lag("site_installed_kw").over(w_read_at_to_date_by_read_at)).alias("site_installed_kw_change"),
        F.round(F.avg("site_installed_kw").over(w_quality_by_read_at_moving_1), 2).alias("site_installed_kw_avg_2"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_quality_site_id), F.lit(0)), 4).alias("site_installed_kw_share_of_quality_site_id"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.sum("site_installed_kw").over(w_site_id_read_at_to_date_by_s).alias("running_site_installed_kw"),
        F.dense_rank().over(w_read_at_to_date_by_read_at_t).alias("read_at_dense_rank"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "read_at",
        "quality_max_site_installed_kw",
        "read_at_quartile",
        "rows_in_quality_site_id",
        "site_installed_kw_change",
        "site_installed_kw_avg_2",
        "rows_in_quality",
        "running_site_installed_kw",
        "read_at_dense_rank",
        F.sum("site_installed_kw_dense_rank").over(w_quality_read_at_dense_rank_b).alias("running_site_installed_kw_dense_rank"),
        F.row_number().over(w_quality_read_at_dense_rank_b_2).alias("site_installed_kw_seq"),
        F.lag("site_installed_kw_avg_2").over(w_read_at_quartile_quality_by).alias("prev_site_installed_kw_avg_2"),
        F.max("read_at_dense_rank").over(w_quality_read_at_dense_rank_b_3).alias("read_at_dense_rank_max_3_prior"),
        F.round(F.col("running_site_installed_kw") - F.avg("running_site_installed_kw").over(w_read_at_quartile_quality), 2).alias("running_site_installed_kw_vs_read_at_quartile_qu"),
    )
)

result = (
    metrics
    .select(
        "id",
        "read_at",
        "quality_max_site_installed_kw",
        "read_at_quartile",
        "rows_in_quality_site_id",
        "site_installed_kw_change",
        "site_installed_kw_avg_2",
        "rows_in_quality",
        "running_site_installed_kw",
        "read_at_dense_rank",
        "running_site_installed_kw_dense_rank",
        "site_installed_kw_seq",
        "prev_site_installed_kw_avg_2",
    )
    .orderBy(F.col("read_at_quartile").desc(), F.col("read_at").desc())
)
