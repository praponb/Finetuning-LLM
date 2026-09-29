from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone = Window.partitionBy("zone")
w_commissioned_on_year_by_inst = Window.partitionBy(F.year("commissioned_on")).orderBy(F.col("installed_kw").desc())
w_tariff = Window.partitionBy("tariff")
w_zone_by_installed_kw = Window.partitionBy("zone").orderBy("installed_kw", "id")
w_all_by_installed_kw = Window.orderBy("installed_kw", "id")
w_all_by_commissioned_on_full = Window.orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone_by_installed_kw_moving = Window.partitionBy("zone").orderBy("installed_kw", "id").rowsBetween(-2, Window.currentRow)
w_zone_tariff_by_commissioned = Window.partitionBy("zone", "tariff").orderBy(F.col("commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_commissioned_on_month_by_ins = Window.partitionBy(F.date_trunc("month", "commissioned_on")).orderBy("installed_kw", "id")
w_all_by_installed_kw_ties = Window.orderBy("installed_kw")
w_all_by_commissioned_on_ties = Window.orderBy("commissioned_on")
w_rn_tariff = Window.partitionBy("rn", "tariff")
w_commissioned_on_dense_rank_b = Window.partitionBy("commissioned_on_dense_rank").orderBy(F.col("installed_kw_share_of_tariff").desc(), "id").rowsBetween(-1, Window.currentRow)
w_first_commissioned_on_year_b = Window.partitionBy(F.year("first_commissioned_on")).orderBy(F.col("tariff_sum_installed_kw").desc(), "id")
w_all_by_total_rows_in_tariff = Window.orderBy(F.col("total_rows_in_tariff").desc_nulls_first(), "installed_kw_rank_2", "seq_no").rowsBetween(-2, Window.currentRow)
w_all_by_total_rows_in_tariff_2 = Window.orderBy(F.col("total_rows_in_tariff").desc_nulls_first(), "installed_kw_rank_2", "seq_no")
w_row_count_seq_no = Window.partitionBy("row_count", "seq_no")
w_installed_kw_rank_2_seq_no_b = Window.partitionBy("installed_kw_rank_2", "seq_no").orderBy("total_rows_in_tariff", "installed_kw_rank_2", "seq_no").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

with_windows = (
    recent
    .select(
        "id",
        "site_name",
        "tariff",
        "commissioned_on",
        F.count(F.lit(1)).over(w_zone).alias("rows_in_zone"),
        F.rank().over(w_commissioned_on_year_by_inst).alias("installed_kw_rank"),
        F.round(F.col("installed_kw") - F.avg("installed_kw").over(w_tariff), 2).alias("installed_kw_vs_tariff_avg"),
        F.round(F.col("installed_kw") / F.nullif(F.sum("installed_kw").over(w_tariff), F.lit(0)), 4).alias("installed_kw_share_of_tariff"),
        F.sum("installed_kw").over(w_tariff).alias("tariff_sum_installed_kw"),
        F.rank().over(w_zone_by_installed_kw).alias("installed_kw_rank_2"),
        F.row_number().over(w_all_by_installed_kw).alias("seq_no"),
        F.round(F.avg("installed_kw").over(w_zone), 2).alias("zone_avg_installed_kw"),
        F.sum("installed_kw").over(w_zone).alias("zone_sum_installed_kw"),
        F.first("commissioned_on").over(w_all_by_commissioned_on_full).alias("first_commissioned_on"),
        F.count(F.lit(1)).over(w_tariff).alias("rows_in_tariff"),
        F.sum("installed_kw").over(w_zone_by_installed_kw_moving).alias("installed_kw_sum_3"),
        F.first("installed_kw", ignorenulls=True).over(w_zone_tariff_by_commissioned).alias("first_known_installed_kw"),
        F.row_number().over(w_commissioned_on_month_by_ins).alias("rn"),
        F.round(F.cume_dist().over(w_all_by_installed_kw_ties), 3).alias("installed_kw_cume_dist"),
        F.dense_rank().over(w_all_by_commissioned_on_ties).alias("commissioned_on_dense_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.round(F.avg("tariff_sum_installed_kw").over(w_rn_tariff), 2).alias("rn_tariff_avg_tariff_sum_installed_kw"),
        F.max("tariff_sum_installed_kw").over(w_rn_tariff).alias("rn_tariff_max_tariff_sum_installed_kw"),
        F.round(F.col("commissioned_on_dense_rank") / F.nullif(F.sum("commissioned_on_dense_rank").over(w_rn_tariff), F.lit(0)), 4).alias("commissioned_on_dense_rank_share_of_rn_tariff"),
        F.round(F.avg("rows_in_tariff").over(w_commissioned_on_dense_rank_b), 2).alias("rows_in_tariff_avg_2"),
        F.count(F.lit(1)).over(w_rn_tariff).alias("rows_in_rn_tariff"),
        F.ntile(10).over(w_first_commissioned_on_year_b).alias("tariff_sum_installed_kw_decile"),
        F.round(F.col("rn") / F.nullif(F.sum("rn").over(w_rn_tariff), F.lit(0)), 4).alias("rn_share_of_rn_tariff"),
    )
)

rollup_base = (
    metrics
    .groupBy("installed_kw_rank_2", "seq_no")
    .agg(
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
        F.sum("rows_in_zone").alias("total_rows_in_zone"),
        F.sum("rows_in_tariff").alias("total_rows_in_tariff"),
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.count("*").alias("row_count"),
    )
)

windowed = (
    rollup_base
    .select(
        "installed_kw_rank_2",
        "seq_no",
        "site_name_list",
        "row_count",
        F.max("total_rows_in_zone").over(w_all_by_total_rows_in_tariff).alias("total_rows_in_zone_max_3"),
        F.row_number().over(w_all_by_total_rows_in_tariff_2).alias("seq_no_2"),
        F.round(F.col("seq_no") - F.avg("seq_no").over(w_row_count_seq_no), 2).alias("seq_no_vs_row_count_seq_no_avg"),
        F.nth_value("row_count", 2).over(w_installed_kw_rank_2_seq_no_b).alias("second_row_count"),
    )
)

result = (
    windowed
    .select(
        "installed_kw_rank_2",
        "seq_no",
        "site_name_list",
        "row_count",
        "total_rows_in_zone_max_3",
        "seq_no_2",
        "seq_no_vs_row_count_seq_no_avg",
        "second_row_count",
    )
    .orderBy(F.col("installed_kw_rank_2").desc(), "second_row_count")
)
