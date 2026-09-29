from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_zone_by_installed_kw = Window.partitionBy("tariff", "zone").orderBy(F.col("installed_kw_adj").desc())
w_tariff_by_installed_kw_adj_f = Window.partitionBy("tariff").orderBy("installed_kw_adj", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_zone = Window.partitionBy("tariff", "zone")
w_all_by_installed_kw_running = Window.orderBy(F.col("installed_kw").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_installed_kw = Window.orderBy(F.col("installed_kw").asc_nulls_last(), "id")
w_tariff_zone_by_installed_kw_2 = Window.partitionBy("tariff", "zone").orderBy(F.col("installed_kw_adj").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff_zone_by_installed_kw_3 = Window.partitionBy("tariff", "zone").orderBy(F.col("installed_kw_adj").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_by_installed_kw_adj_m = Window.partitionBy("tariff").orderBy("installed_kw_adj", "id").rowsBetween(-1, Window.currentRow)
w_tariff_by_installed_kw_adj = Window.partitionBy("tariff").orderBy("installed_kw_adj", "id")
w_seq_no_by_first_installed_kw = Window.partitionBy("seq_no").orderBy("first_installed_kw_adj", "id")
w_seq_no = Window.partitionBy("seq_no")
w_seq_no_by_first_installed_kw_2 = Window.partitionBy("seq_no").orderBy("first_installed_kw_adj_distance", "id")
w_seq_no_by_first_installed_kw_3 = Window.partitionBy("seq_no").orderBy("first_installed_kw_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_seq_no_by_first_installed_kw_4 = Window.partitionBy("seq_no").orderBy("first_installed_kw_adj", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_sites
    .filter((F.col("commissioned_on") < F.lit("2024-11-21").cast("date")) & (F.col("tariff") == "FLAT"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        F.round(F.col("installed_kw") * 0.25, 2).alias("installed_kw_adj"),
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "installed_kw_adj",
        F.dense_rank().over(w_tariff_zone_by_installed_kw).alias("installed_kw_adj_dense_rank"),
        F.first("installed_kw_adj").over(w_tariff_by_installed_kw_adj_f).alias("first_installed_kw_adj"),
        F.first("installed_kw", ignorenulls=True).over(w_all_by_installed_kw_running).alias("first_known_installed_kw"),
        (F.col("installed_kw_adj") - F.lag("installed_kw_adj").over(w_all_by_installed_kw)).alias("installed_kw_adj_change"),
        F.first("installed_kw").over(w_tariff_by_installed_kw_adj_f).alias("first_installed_kw"),
        F.sum("installed_kw").over(w_tariff_zone_by_installed_kw_2).alias("running_installed_kw"),
        F.last("installed_kw").over(w_tariff_zone_by_installed_kw_3).alias("last_installed_kw"),
        F.sum("installed_kw").over(w_all_by_installed_kw_running).alias("running_installed_kw_2"),
        F.round(F.percent_rank().over(w_all_by_installed_kw), 3).alias("installed_kw_percent_rank"),
        F.round(F.col("installed_kw_adj") / F.nullif(F.sum("installed_kw_adj").over(w_tariff_zone), F.lit(0)), 4).alias("installed_kw_adj_share_of_tariff_zone"),
        F.round(F.avg("installed_kw_adj").over(w_tariff_by_installed_kw_adj_m), 2).alias("installed_kw_adj_avg_2"),
        F.row_number().over(w_tariff_by_installed_kw_adj).alias("seq_no"),
    )
)

scored = (
    ranked
    .filter(F.col("installed_kw_adj") < 526.5)
    .select(
        "id",
        "installed_kw",
        "installed_kw_adj",
        "first_installed_kw_adj",
        "installed_kw_adj_change",
        "first_installed_kw",
        "last_installed_kw",
        "installed_kw_adj_avg_2",
        "seq_no",
        F.abs(F.col("first_installed_kw_adj") - 414.8).alias("first_installed_kw_adj_distance"),
        F.substring(F.trim(F.col("site_name")), 1, 3).alias("site_name_prefix"),
    )
)

windowed = (
    scored
    .select(
        "id",
        "installed_kw_adj",
        "first_installed_kw_adj",
        "installed_kw_adj_change",
        "last_installed_kw",
        "installed_kw_adj_avg_2",
        "seq_no",
        "first_installed_kw_adj_distance",
        F.round(F.cume_dist().over(w_seq_no_by_first_installed_kw), 3).alias("first_installed_kw_adj_cume_dist"),
        F.lag("first_installed_kw", 3).over(w_seq_no_by_first_installed_kw).alias("prev3_first_installed_kw"),
        F.round(F.col("last_installed_kw") - F.avg("last_installed_kw").over(w_seq_no), 2).alias("last_installed_kw_vs_seq_no_avg"),
        F.lag("seq_no").over(w_seq_no_by_first_installed_kw_2).alias("prev_seq_no"),
        F.first("seq_no", ignorenulls=True).over(w_seq_no_by_first_installed_kw_3).alias("first_known_seq_no"),
        F.first("installed_kw").over(w_seq_no_by_first_installed_kw_4).alias("first_installed_kw_2"),
        F.lag("installed_kw_adj", 2).over(w_seq_no_by_first_installed_kw).alias("prev2_installed_kw_adj"),
    )
)

result = (
    windowed
    .filter(F.col("prev_seq_no") < 7)
    .select(
        "id",
        "first_installed_kw_adj",
        "last_installed_kw",
        "installed_kw_adj_avg_2",
        "seq_no",
        "first_installed_kw_adj_cume_dist",
        "last_installed_kw_vs_seq_no_avg",
        "first_installed_kw_2",
        "prev2_installed_kw_adj",
        F.coalesce(F.col("installed_kw_adj_change"), F.lit(0.0)).alias("installed_kw_adj_change_filled"),
    )
)
