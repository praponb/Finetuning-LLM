from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_month_by_rate_adj = Window.partitionBy("valid_to_month").orderBy("rate_adj", "id")
w_all_by_rate_ties = Window.orderBy("rate")
w_valid_to_month = Window.partitionBy("valid_to_month")
w_all_by_rate_running = Window.orderBy("rate", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tariff_windows
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.round(F.col("rate") * 0.9, 2).alias("rate_adj"),
        F.month(F.col("valid_to")).alias("valid_to_month"),
    )
)

filtered = (
    prepared
    .crossJoin(prepared.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

metrics = (
    filtered
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        "rate_adj",
        "valid_to_month",
        (F.col("rate_adj") - F.lag("rate_adj").over(w_valid_to_month_by_rate_adj)).alias("rate_adj_change"),
        F.round(F.percent_rank().over(w_all_by_rate_ties), 3).alias("rate_percent_rank"),
        F.max("rate").over(w_valid_to_month).alias("valid_to_month_max_rate"),
        F.sum("rate_adj").over(w_valid_to_month).alias("valid_to_month_sum_rate_adj"),
        F.first("valid_to_month", ignorenulls=True).over(w_all_by_rate_running).alias("first_known_valid_to_month"),
    )
)

result = (
    metrics
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        "rate_adj",
        "valid_to_month",
        "rate_adj_change",
        "rate_percent_rank",
        "valid_to_month_max_rate",
        "first_known_valid_to_month",
        (F.floor(F.col("valid_to_month_max_rate") / 10) * 10).alias("valid_to_month_max_rate_bucket"),
    )
    .orderBy(
        F.col("rate_adj_change").desc(),
        F.col("first_known_valid_to_month").asc_nulls_last(),
    )
)
