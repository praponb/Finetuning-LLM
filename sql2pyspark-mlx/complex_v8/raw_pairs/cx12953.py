from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_month_tariff_len_by = Window.partitionBy(F.month("valid_to"), "tariff_len").orderBy(F.col("valid_to").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_valid_to_to_date_by_valid_to = Window.partitionBy(F.to_date("valid_to")).orderBy(F.col("valid_to").desc())
w_tariff_len_tariff_or_unknown = Window.partitionBy("tariff_len", "tariff_or_unknown").orderBy("valid_from", "id")
w_all_by_valid_from_prior_1 = Window.orderBy(F.col("valid_from").desc(), "id").rowsBetween(-1, -1)
w_all_by_valid_from = Window.orderBy(F.col("valid_from").desc(), "id")
w_valid_from_month = Window.partitionBy(F.month("valid_from"))
w_tariff_len = Window.partitionBy("tariff_len")
w_all_by_valid_from_moving_2 = Window.orderBy(F.col("valid_from").desc(), "id").rowsBetween(-2, Window.currentRow)
w_valid_to_month_tariff_len_by_2 = Window.partitionBy(F.month("valid_to"), "tariff_len").orderBy(F.col("valid_to").asc_nulls_last(), "id")
w_all_by_rate_running = Window.orderBy(F.col("rate").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_valid_to_month_tariff_len_by_3 = Window.partitionBy(F.month("valid_to"), "tariff_len").orderBy(F.col("valid_to").asc_nulls_last())
w_tariff_or_unknown = Window.partitionBy("tariff_or_unknown")
w_next_valid_from_to_date_by_r = Window.partitionBy(F.to_date("next_valid_from")).orderBy("rate", "id")
w_valid_to_month_tariff_len = Window.partitionBy(F.date_trunc("month", "valid_to"), "tariff_len")
w_all_by_distinct_tariff_or_un = Window.orderBy("distinct_tariff_or_unknown_in_tariff_len", "id").rowsBetween(-1, -1)

filtered_tariffs = (
    df_tariff_windows
    .filter((F.col("valid_to") < F.lit("2025-03-12 23:00:00").cast("timestamp")) & (F.col("rate") > 4.2))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

with_windows = (
    filtered_tariffs
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        "tariff_len",
        F.max("rate").over(w_valid_to_month_tariff_len_by).alias("max_rate_so_far"),
        F.dense_rank().over(w_valid_to_to_date_by_valid_to).alias("valid_to_dense_rank"),
        F.round(F.percent_rank().over(w_tariff_len_tariff_or_unknown), 3).alias("valid_from_percent_rank"),
        F.max("tariff_len").over(w_all_by_valid_from_prior_1).alias("tariff_len_max_1_prior"),
        F.lead("rate").over(w_all_by_valid_from).alias("next_rate"),
        F.max("rate").over(w_valid_from_month).alias("valid_from_month_max_rate"),
        F.size(F.collect_set("tariff_or_unknown").over(w_tariff_len)).alias("distinct_tariff_or_unknown_in_tariff_len"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_valid_from_month), F.lit(0)), 4).alias("rate_share_of_valid_from_month"),
        F.lead("valid_from", 2).over(w_all_by_valid_from).alias("next2_valid_from"),
        F.sum("tariff_len").over(w_all_by_valid_from_moving_2).alias("tariff_len_sum_3"),
        F.rank().over(w_valid_to_month_tariff_len_by_2).alias("valid_to_rank"),
        F.lead("valid_from").over(w_all_by_valid_from).alias("next_valid_from"),
        F.round(F.avg("tariff_len").over(w_valid_from_month), 2).alias("valid_from_month_avg_tariff_len"),
        F.first("tariff_len", ignorenulls=True).over(w_all_by_rate_running).alias("first_known_tariff_len"),
        F.round(F.cume_dist().over(w_valid_to_month_tariff_len_by_3), 3).alias("valid_to_cume_dist"),
        F.min("tariff_len").over(w_tariff_or_unknown).alias("tariff_or_unknown_min_tariff_len"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "valid_from",
        "valid_to",
        "tariff_len",
        "max_rate_so_far",
        "valid_to_dense_rank",
        "valid_from_percent_rank",
        "tariff_len_max_1_prior",
        "next_rate",
        "distinct_tariff_or_unknown_in_tariff_len",
        "rate_share_of_valid_from_month",
        "next2_valid_from",
        "tariff_len_sum_3",
        "valid_to_rank",
        "next_valid_from",
        "valid_from_month_avg_tariff_len",
        "valid_to_cume_dist",
        "tariff_or_unknown_min_tariff_len",
        F.rank().over(w_next_valid_from_to_date_by_r).alias("rate_rank"),
        F.count(F.lit(1)).over(w_valid_to_month_tariff_len).alias("rows_in_valid_to_month_tariff_len"),
        F.round(F.avg("valid_to_cume_dist").over(w_all_by_distinct_tariff_or_un), 2).alias("valid_to_cume_dist_avg_1_prior"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "valid_from",
        "valid_to",
        "tariff_len",
        "max_rate_so_far",
        "valid_from_percent_rank",
        "tariff_len_max_1_prior",
        "rate_share_of_valid_from_month",
        "next2_valid_from",
        "tariff_len_sum_3",
        "valid_to_rank",
        "valid_to_cume_dist",
        "tariff_or_unknown_min_tariff_len",
        "rate_rank",
        "rows_in_valid_to_month_tariff_len",
        "valid_to_cume_dist_avg_1_prior",
        F.year(F.col("next2_valid_from")).alias("next2_valid_from_year"),
    )
    .orderBy("valid_to_cume_dist", F.col("max_rate_so_far").asc(), "id")
    .limit(10)
)
