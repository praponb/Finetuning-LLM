from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_quarter_read_at_mont = Window.partitionBy("read_at_quarter", F.month("read_at"))
w_site_zone_quality_len_by_kwh = Window.partitionBy("site_zone", "quality_len").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_quality_len = Window.orderBy(F.col("quality_len").desc(), "id")
w_read_at_quarter = Window.partitionBy("read_at_quarter")
w_read_at_quarter_by_read_at_q = Window.partitionBy("read_at_quarter").orderBy("read_at_quarter", "id")
w_quality_len_quality_by_kwh = Window.partitionBy("quality_len", "quality").orderBy(F.col("kwh").desc(), "id")
w_read_at_quarter_rank_read_at = Window.partitionBy("read_at_quarter_rank", "read_at_quarter")
w_all_by_read_at_quarter_rank = Window.orderBy("read_at_quarter_rank", "id").rowsBetween(-1, Window.currentRow)
w_all_by_read_at_quarter_rank_2 = Window.orderBy("read_at_quarter_rank")
w_quality_len_quality = Window.partitionBy("quality_len", "quality")
w_all_by_distinct_ids_full = Window.orderBy("distinct_ids", "quality_len", "quality_len_tercile", "quality").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_meter_readings
    .filter(F.col("quality") != "BAD")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

joined = (
    valid.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.round(F.avg("kwh").over(w_read_at_quarter_read_at_mont), 2).alias("read_at_quarter_read_at_month_avg_kwh"),
        F.first("read_at", ignorenulls=True).over(w_site_zone_quality_len_by_kwh).alias("first_known_read_at"),
        F.ntile(3).over(w_all_by_quality_len).alias("quality_len_tercile"),
        F.round(F.avg("quality_len").over(w_read_at_quarter_read_at_mont), 2).alias("read_at_quarter_read_at_month_avg_quality_len"),
        F.size(F.collect_set("quality").over(w_read_at_quarter)).alias("distinct_quality_in_read_at_quarter"),
        F.rank().over(w_read_at_quarter_by_read_at_q).alias("read_at_quarter_rank"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_quarter_read_at_mont), F.lit(0)), 4).alias("kwh_share_of_read_at_quarter_read_at_month"),
        F.rank().over(w_quality_len_quality_by_kwh).alias("kwh_rank"),
    )
)

top_rows = with_windows.filter(F.col("read_at_quarter_rank") <= 2)

metrics = (
    top_rows
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "read_at_quarter",
        "quality_len",
        "site_commissioned_on",
        "site_zone",
        "read_at_quarter_read_at_month_avg_kwh",
        "first_known_read_at",
        "quality_len_tercile",
        "distinct_quality_in_read_at_quarter",
        "read_at_quarter_rank",
        "kwh_rank",
        F.max("kwh_rank").over(w_read_at_quarter_rank_read_at).alias("read_at_quarter_rank_read_at_quarter_max_kwh_ran"),
        F.round(F.avg("kwh_rank").over(w_all_by_read_at_quarter_rank), 2).alias("kwh_rank_avg_2"),
        F.round(F.percent_rank().over(w_all_by_read_at_quarter_rank_2), 3).alias("read_at_quarter_rank_percent_rank"),
        F.min("kwh_rank").over(w_read_at_quarter_rank_read_at).alias("read_at_quarter_rank_read_at_quarter_min_kwh_ran"),
    )
)

stats = (
    metrics
    .groupBy("quality_len", "quality_len_tercile", "quality")
    .agg(
        F.array_sort(F.collect_set("site_zone")).alias("site_zone_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("read_at_quarter_rank_read_at_quarter_min_kwh_ran").alias("total_read_at_quarter_rank_read_at_quarter_min_k"),
    )
)

with_windows_2 = (
    stats
    .select(
        "quality_len",
        "quality_len_tercile",
        "quality",
        "site_zone_list",
        "distinct_ids",
        F.count(F.lit(1)).over(w_quality_len_quality).alias("rows_in_quality_len_quality"),
        F.sum("quality_len").over(w_quality_len_quality).alias("quality_len_quality_sum_quality_len"),
        F.last("quality_len_tercile").over(w_all_by_distinct_ids_full).alias("last_quality_len_tercile"),
    )
)

result = (
    with_windows_2
    .select("quality_len", "rows_in_quality_len_quality")
    .distinct()
    .orderBy(
        F.col("rows_in_quality_len_quality").desc(),
        F.col("quality_len").desc(),
    )
)
