from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_plus_7d_month_tarif = Window.partitionBy(F.month("valid_to_plus_7d"), "tariff").orderBy(F.col("valid_from").desc(), "id")
w_tariff = Window.partitionBy("tariff")
w_all_by_tariff_max_rate = Window.orderBy("tariff_max_rate", "id")
w_valid_from_rank = Window.partitionBy("valid_from_rank")
w_all_by_tariff_max_rate_runni = Window.orderBy("tariff_max_rate", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tariff_windows
    .filter(F.col("tariff") == "EV")
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        (F.col("valid_to") + F.expr("INTERVAL 7 DAYS")).alias("valid_to_plus_7d"),
    )
)

sequenced = (
    prepared
    .select(
        "id",
        "tariff",
        "rate",
        "valid_to_plus_7d",
        F.rank().over(w_valid_to_plus_7d_month_tarif).alias("valid_from_rank"),
        F.round(F.avg("rate").over(w_tariff), 2).alias("tariff_avg_rate"),
        F.round((F.unix_timestamp("valid_from") - F.unix_timestamp(F.lag("valid_from").over(w_valid_to_plus_7d_month_tarif))) / 60, 1).alias("minutes_since_prev_valid_from"),
        F.max("rate").over(w_tariff).alias("tariff_max_rate"),
    )
)

qualified = (
    sequenced
    .crossJoin(sequenced.agg(F.max("tariff_max_rate").alias("overall_max_tariff_max_rate")))
    .filter(F.col("tariff_max_rate") < F.col("overall_max_tariff_max_rate"))
    .drop("overall_max_tariff_max_rate")
)

tagged = (
    qualified
    .filter(F.col("tariff_avg_rate") > 4.4)
    .select(
        "id",
        "rate",
        "valid_from_rank",
        "tariff_max_rate",
        F.round(F.col("tariff_avg_rate") * 0.9, 2).alias("tariff_avg_rate_adj"),
    )
)

screened = (
    tagged
    .crossJoin(tagged.agg(F.avg("tariff_avg_rate_adj").alias("overall_avg_tariff_avg_rate_adj")))
    .filter(F.col("tariff_avg_rate_adj") > F.col("overall_avg_tariff_avg_rate_adj"))
    .drop("overall_avg_tariff_avg_rate_adj")
)

windowed = (
    screened
    .select(
        "*",
        F.row_number().over(w_all_by_tariff_max_rate).alias("tariff_max_rate_seq"),
        F.count(F.lit(1)).over(w_valid_from_rank).alias("rows_in_valid_from_rank"),
        F.last("tariff_avg_rate_adj", ignorenulls=True).over(w_all_by_tariff_max_rate_runni).alias("last_known_tariff_avg_rate_adj"),
    )
)

result = (
    windowed
    .select(
        "id",
        "tariff_max_rate_seq",
        "valid_from_rank",
        "rows_in_valid_from_rank",
    )
    .unpivot(
        ids=["id"],
        values=["tariff_max_rate_seq", "valid_from_rank", "rows_in_valid_from_rank"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric_name")
)
