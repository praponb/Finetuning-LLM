from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_site_zone_site_id_by_read_at = Window.partitionBy("site_zone", "site_id").orderBy(F.col("read_at").desc(), "id")
w_site_zone = Window.partitionBy("site_zone")
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone_site_id_by_read_at_2 = Window.partitionBy("site_zone", "site_id").orderBy(F.col("read_at").asc_nulls_last(), "id")
w_site_id = Window.partitionBy("site_id")
w_all_by_read_at_full = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_by_median_distinct_si = Window.partitionBy("n_rows").orderBy("median_distinct_site_id_in_quality", "read_at_dense_rank", "read_at_tercile", "read_at_ym")
w_read_at_tercile_n_rows = Window.partitionBy("read_at_tercile", "n_rows")
w_median_distinct_site_id_in_q = Window.partitionBy("median_distinct_site_id_in_quality").orderBy(F.col("n_rows").desc())
w_median_distinct_site_id_in_q_2 = Window.partitionBy("median_distinct_site_id_in_quality").orderBy(F.col("n_rows").desc(), "read_at_dense_rank", "read_at_tercile", "read_at_ym")

scoped = (
    df_meter_readings
    .filter(~F.col("quality").isin("BAD", "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

joined = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") >= 141.6)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "site_id",
        "read_at",
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.ntile(3).over(w_site_zone_site_id_by_read_at).alias("read_at_tercile"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_zone), F.lit(0)), 4).alias("kwh_share_of_site_zone"),
        F.min("kwh").over(w_all_by_read_at_running).alias("min_kwh_so_far"),
        F.count(F.lit(1)).over(w_site_zone).alias("rows_in_site_zone"),
        F.sum("kwh").over(w_quality).alias("quality_sum_kwh"),
        F.dense_rank().over(w_site_zone_site_id_by_read_at_2).alias("read_at_dense_rank"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_zone), F.lit(0)), 4).alias("site_installed_kw_share_of_site_zone"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_quality), 2).alias("site_installed_kw_vs_quality_avg"),
        F.nth_value("site_installed_kw", 2).over(w_all_by_read_at_full).alias("second_site_installed_kw"),
    )
)

annotated = (
    with_windows
    .filter(F.col("read_at_dense_rank").between(4, 6))
    .select(
        "*",
        F.greatest(F.col("quality_sum_kwh"), F.col("read_at_tercile")).alias("greatest_quality_sum_kwh_read_at_tercile"),
        F.coalesce(F.col("min_kwh_so_far"), F.lit(0.0)).alias("min_kwh_so_far_filled"),
        F.greatest(F.col("min_kwh_so_far"), F.col("read_at_tercile")).alias("greatest_min_kwh_so_far_read_at_tercile"),
    )
)

grouped = (
    annotated
    .filter(F.col("read_at_tercile") > 4)
    .groupBy(
        "read_at_dense_rank",
        "read_at_tercile",
        F.date_format("read_at", "yyyy-MM").alias("read_at_ym"),
    )
    .agg(
        F.round(F.sum("read_at_tercile") / F.nullif(F.sum("rows_in_site_id"), F.lit(0)), 3).alias("read_at_tercile_to_rows_in_site_id_ratio"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("distinct_site_id_in_quality", 0.5).alias("median_distinct_site_id_in_quality"),
    )
)

ranked = (
    grouped
    .select(
        "*",
        F.dense_rank().over(w_n_rows_by_median_distinct_si).alias("median_distinct_site_id_in_quality_dense_rank"),
        F.round(F.avg("read_at_tercile").over(w_read_at_tercile_n_rows), 2).alias("read_at_tercile_n_rows_avg_read_at_tercile"),
        F.round(F.percent_rank().over(w_median_distinct_site_id_in_q), 3).alias("n_rows_percent_rank"),
        F.row_number().over(w_median_distinct_site_id_in_q_2).alias("n_rows_seq"),
    )
)

result = (
    ranked
    .select(
        "read_at_dense_rank",
        "read_at_tercile",
        "read_at_ym",
        "read_at_tercile_to_rows_in_site_id_ratio",
        "n_rows",
        "median_distinct_site_id_in_quality",
        "read_at_tercile_n_rows_avg_read_at_tercile",
        "n_rows_percent_rank",
        "n_rows_seq",
    )
)
