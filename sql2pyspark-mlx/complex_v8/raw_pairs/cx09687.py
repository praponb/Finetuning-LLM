from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").desc_nulls_first(), "id")
w_quality = Window.partitionBy("quality")
w_site_id_by_read_at_moving_1 = Window.partitionBy("site_id").orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_site_id_by_site_installed_kw = Window.partitionBy("site_id").orderBy(F.col("site_installed_kw").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_site_id_by_site_installed_kw_2 = Window.partitionBy("site_id").orderBy(F.col("site_installed_kw").asc_nulls_last(), "id")
w_all_by_read_at = Window.orderBy("read_at", "id")
w_all_by_read_at_running = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_site_id_in_quality = Window.partitionBy("distinct_site_id_in_quality", "read_at_quartile").orderBy(F.col("distinct_site_id_in_quality").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_distinct_site_id_in_q = Window.orderBy("distinct_site_id_in_quality", "id")
w_all_by_read_at_ties = Window.orderBy("read_at")
w_distinct_site_id_in_quality_2 = Window.partitionBy("distinct_site_id_in_quality", "read_at_quartile").orderBy(F.col("distinct_site_id_in_quality").desc(), "id")
w_all_by_read_at_cume_dist = Window.orderBy("read_at_cume_dist", "id")

cleaned = (
    df_meter_readings
    .filter((F.col("quality").isin("BAD", "EST")) | (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("voltage") >= 232.4, "A").when(F.col("voltage") >= 220.7, "B").otherwise("C").alias("voltage_tier"),
    )
)

with_parent_site = (
    cleaned.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

metrics = (
    with_parent_site
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "site_installed_kw",
        F.ntile(4).over(w_site_id_by_read_at).alias("read_at_quartile"),
        F.round(F.cume_dist().over(w_site_id_by_read_at), 3).alias("read_at_cume_dist"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_quality_site_id), F.lit(0)), 4).alias("site_installed_kw_share_of_quality_site_id"),
        F.round(F.avg("site_installed_kw").over(w_site_id_by_read_at_moving_1), 2).alias("site_installed_kw_avg_2"),
        F.max("site_installed_kw").over(w_site_id_by_site_installed_kw).alias("site_installed_kw_max_1_prior"),
        F.row_number().over(w_site_id_by_site_installed_kw_2).alias("site_installed_kw_seq"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.min("site_installed_kw").over(w_all_by_read_at_running).alias("min_site_installed_kw_so_far"),
    )
)

leaders = (
    metrics
    .filter(F.col("site_installed_kw_seq") <= 3)
    .drop("site_installed_kw_seq")
)

windowed = (
    leaders
    .select(
        "*",
        F.round(F.avg("read_at_quartile").over(w_distinct_site_id_in_quality), 2).alias("read_at_quartile_avg_7"),
        F.row_number().over(w_all_by_distinct_site_id_in_q).alias("seq_no"),
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.lag("site_installed_kw_avg_2").over(w_all_by_read_at).alias("prev_site_installed_kw_avg_2"),
        F.lag("read_at_cume_dist", 2).over(w_distinct_site_id_in_quality_2).alias("prev2_read_at_cume_dist"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality_2"),
        F.lag("rows_in_quality").over(w_all_by_read_at_cume_dist).alias("prev_rows_in_quality"),
        (F.col("distinct_site_id_in_quality") - F.lag("distinct_site_id_in_quality").over(w_all_by_read_at_cume_dist)).alias("distinct_site_id_in_quality_change"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "read_at",
        "site_installed_kw",
        "read_at_quartile",
        "read_at_cume_dist",
        "rows_in_quality",
        "site_installed_kw_share_of_quality_site_id",
        "site_installed_kw_avg_2",
        "minutes_since_prev_read_at",
        "min_site_installed_kw_so_far",
        "read_at_quartile_avg_7",
        "prev2_read_at_cume_dist",
        "rows_in_quality_2",
        "prev_rows_in_quality",
        "distinct_site_id_in_quality_change",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("min_site_installed_kw_so_far").desc(),
        "id",
    )
    .limit(5)
)
