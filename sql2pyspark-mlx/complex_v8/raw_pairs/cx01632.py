from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_valid_to_running = Window.orderBy("valid_to", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_valid_to_ties = Window.orderBy("valid_to")
w_all_by_valid_to = Window.orderBy("valid_to", "id")
w_valid_to_month_first_known_v = Window.partitionBy(F.month("valid_to"), F.to_date("first_known_valid_from")).orderBy("valid_from", "id")
w_valid_to_month_first_known_v_2 = Window.partitionBy(F.month("valid_to"), F.to_date("first_known_valid_from")).orderBy("valid_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_valid_to_month_first_known_v_3 = Window.partitionBy(F.month("valid_to"), F.to_date("first_known_valid_from")).orderBy("valid_from", "id").rowsBetween(-3, Window.currentRow)
w_valid_to_month_first_known_v_4 = Window.partitionBy(F.month("valid_to"), F.to_date("first_known_valid_from")).orderBy("valid_from")
w_tariff_valid_to_month = Window.partitionBy("tariff", F.date_trunc("month", "valid_to"))

cleaned = (
    df_tariff_windows
    .filter((F.col("valid_from") >= F.lit("2025-01-24 14:00:00").cast("timestamp")) & (~F.col("tariff").isin("EV", "TOU")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
    )
)

derived = (
    cleaned
    .filter(F.col("tariff") != "FLAT")
    .select(
        "*",
        F.date_format(F.col("valid_from"), "yyyy-MM").alias("valid_from_label"),
        F.date_format(F.col("valid_to"), "MMM yyyy").alias("valid_to_label"),
    )
)

with_windows = (
    derived
    .select(
        "*",
        F.last("valid_to", ignorenulls=True).over(w_all_by_valid_to_running).alias("last_known_valid_to"),
        F.first("valid_from", ignorenulls=True).over(w_all_by_valid_to_running).alias("first_known_valid_from"),
        F.rank().over(w_all_by_valid_to_ties).alias("valid_to_rank"),
        F.round((F.unix_timestamp("valid_from") - F.unix_timestamp(F.lag("valid_from").over(w_all_by_valid_to))) / 60, 1).alias("minutes_since_prev_valid_from"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_from_label",
        "valid_to_label",
        "first_known_valid_from",
        "minutes_since_prev_valid_from",
        F.dense_rank().over(w_valid_to_month_first_known_v).alias("valid_from_dense_rank"),
        F.last("valid_to_rank", ignorenulls=True).over(w_valid_to_month_first_known_v_2).alias("last_known_valid_to_rank"),
        F.round(F.avg("valid_to_rank").over(w_valid_to_month_first_known_v_3), 2).alias("valid_to_rank_avg_4"),
        F.dense_rank().over(w_valid_to_month_first_known_v_4).alias("valid_from_dense_rank_2"),
        F.ntile(10).over(w_valid_to_month_first_known_v).alias("valid_from_decile"),
        F.round(F.col("valid_to_rank") - F.avg("valid_to_rank").over(w_tariff_valid_to_month), 2).alias("valid_to_rank_vs_tariff_valid_to_month_avg"),
    )
)

result = (
    with_trends
    .cube("tariff", "valid_from_dense_rank_2")
    .agg(F.grouping_id().alias("gid"), F.count("*").alias("n_rows"))
    .select(
        F.coalesce(F.col("tariff"), F.lit("ALL")).alias("tariff"),
        "valid_from_dense_rank_2",
        "gid",
        "n_rows",
    )
    .orderBy(
        F.col("tariff").asc_nulls_last(),
        F.col("valid_from_dense_rank_2").asc_nulls_last(),
    )
)
