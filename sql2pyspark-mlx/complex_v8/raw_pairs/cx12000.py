from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_valid_to = Window.orderBy(F.col("valid_to").desc(), "id")
w_all_by_valid_to_full = Window.orderBy(F.col("valid_to").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_valid_to_moving_1 = Window.orderBy(F.col("valid_to").desc(), "id").rowsBetween(-1, Window.currentRow)
w_distinct_ids_last_next2_vali = Window.partitionBy("distinct_ids", F.month("last_next2_valid_to"))
w_all_by_cnt_full = Window.orderBy(F.col("cnt").desc(), "next2_valid_to_date_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_cnt_running = Window.orderBy(F.col("cnt").desc(), "next2_valid_to_date_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_ids = Window.partitionBy("distinct_ids")
w_distinct_ids_by_distinct_ids = Window.partitionBy("distinct_ids").orderBy(F.col("distinct_ids").desc_nulls_first(), "next2_valid_to_date_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_tariffs = (
    df_tariff_windows
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
    )
)

with_trends = (
    valid_tariffs
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.lag("rate").over(w_all_by_valid_to).alias("prev_rate"),
        F.last("rate").over(w_all_by_valid_to_full).alias("last_rate"),
        F.max("rate").over(w_all_by_valid_to_moving_1).alias("rate_max_2"),
        F.lead("valid_to", 2).over(w_all_by_valid_to).alias("next2_valid_to"),
    )
)

derived = (
    with_trends
    .filter(F.col("rate_max_2") > 10.8)
    .select(
        "id",
        "valid_to",
        "rate",
        "last_rate",
        "next2_valid_to",
        F.date_format(F.col("next2_valid_to"), "MMM yyyy").alias("next2_valid_to_label"),
        F.to_date(F.col("next2_valid_to")).alias("next2_valid_to_date"),
    )
)

stats = (
    derived
    .groupBy(
        F.date_format("next2_valid_to_date", "yyyy-MM").alias("next2_valid_to_date_ym"),
    )
    .agg(
        F.min("valid_to").alias("first_valid_to"),
        F.max("next2_valid_to").alias("last_next2_valid_to"),
        F.sum("rate").alias("total_rate"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

metrics = (
    stats
    .select(
        "*",
        F.last("first_valid_to").over(w_all_by_cnt_full).alias("last_first_valid_to"),
        F.sum("distinct_ids").over(w_distinct_ids_last_next2_vali).alias("distinct_ids_last_next2_valid_to_month_sum_disti"),
        F.first("total_rate", ignorenulls=True).over(w_all_by_cnt_running).alias("first_known_total_rate"),
        F.round(F.col("cnt") - F.avg("cnt").over(w_distinct_ids_last_next2_vali), 2).alias("cnt_vs_distinct_ids_last_next2_valid_to_month_av"),
    )
)

tagged = (
    metrics
    .filter(F.col("last_next2_valid_to") >= F.lit("2025-02-15 04:00:00").cast("timestamp"))
    .select(
        "next2_valid_to_date_ym",
        "first_valid_to",
        "last_next2_valid_to",
        "total_rate",
        "distinct_ids",
        "last_first_valid_to",
        "distinct_ids_last_next2_valid_to_month_sum_disti",
        "first_known_total_rate",
        F.concat(F.col("next2_valid_to_date_ym"), F.lit("#"), F.col("cnt").cast("string")).alias("next2_valid_to_date_ym_ref"),
    )
)

windowed = (
    tagged
    .select(
        "next2_valid_to_date_ym",
        "first_valid_to",
        "total_rate",
        "distinct_ids",
        "last_first_valid_to",
        "first_known_total_rate",
        "next2_valid_to_date_ym_ref",
        F.last("last_next2_valid_to").over(w_distinct_ids_by_distinct_ids).alias("last_last_next2_valid_to"),
        F.round(F.col("total_rate") / F.nullif(F.sum("total_rate").over(w_distinct_ids), F.lit(0)), 4).alias("total_rate_share_of_distinct_ids"),
        F.round(F.avg("distinct_ids").over(w_distinct_ids), 2).alias("distinct_ids_avg_distinct_ids"),
        F.count(F.lit(1)).over(w_distinct_ids).alias("rows_in_distinct_ids"),
    )
)

result = (
    windowed
    .select(
        "next2_valid_to_date_ym",
        "total_rate",
        "distinct_ids",
        "last_first_valid_to",
        "first_known_total_rate",
        "next2_valid_to_date_ym_ref",
        "last_last_next2_valid_to",
        "distinct_ids_avg_distinct_ids",
        "rows_in_distinct_ids",
        F.round((F.unix_timestamp(F.col("last_last_next2_valid_to")) - F.unix_timestamp(F.col("first_valid_to"))) / 60, 1).alias("minutes_first_valid_to_to_last_last_next2_valid_"),
    )
    .orderBy("last_last_next2_valid_to")
)
