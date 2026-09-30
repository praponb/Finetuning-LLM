from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_cnt_read_at_month_month = Window.partitionBy("cnt", F.month("read_at_month"))
w_all_by_cnt_running = Window.orderBy(F.col("cnt").desc(), "site_id", "read_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_meter_readings
    .filter((F.col("quality").isin("OK", "BAD")) & (F.col("read_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.round(F.col("kwh") * 1.1, 2).alias("kwh_adj"),
    )
)

windowed = (
    valid
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "kwh_adj",
        F.lag("read_at", 3).over(w_all_by_kwh).alias("prev3_read_at"),
        F.first("kwh").over(w_site_id_by_kwh_full).alias("first_kwh"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.lead("kwh_adj").over(w_site_id_by_kwh).alias("next_kwh_adj"),
        F.rank().over(w_all_by_kwh).alias("kwh_rank"),
        F.min("kwh").over(w_quality_site_id).alias("quality_site_id_min_kwh"),
    )
)

rollup_base = (
    windowed
    .groupBy("site_id", F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("first_kwh"), 2).alias("total_first_kwh"),
        F.sum(F.when(F.col("quality") == "EST", F.col("first_kwh")).otherwise(0)).alias("first_kwh_quality_est"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
)

sequenced = (
    rollup_base
    .select(
        "site_id",
        "read_at_month",
        "cnt",
        "quality_list",
        F.round(F.col("first_kwh_quality_est") - F.avg("first_kwh_quality_est").over(w_cnt_read_at_month_month), 2).alias("first_kwh_quality_est_vs_cnt_read_at_month_month"),
        F.first("first_kwh_quality_est", ignorenulls=True).over(w_all_by_cnt_running).alias("first_known_first_kwh_quality_est"),
        F.count(F.lit(1)).over(w_cnt_read_at_month_month).alias("rows_in_cnt_read_at_month_month"),
        F.sum("first_kwh_quality_est").over(w_cnt_read_at_month_month).alias("cnt_read_at_month_month_sum_first_kwh_quality_es"),
    )
)

result = (
    sequenced
    .select(
        "site_id",
        "cnt",
        "quality_list",
        "first_kwh_quality_est_vs_cnt_read_at_month_month",
        "first_known_first_kwh_quality_est",
        "rows_in_cnt_read_at_month_month",
        "cnt_read_at_month_month_sum_first_kwh_quality_es",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at_month")).alias("days_since_read_at_month"),
    )
    .orderBy(F.col("site_id").desc())
)
