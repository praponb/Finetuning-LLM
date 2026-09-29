from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_valid_from_date_month = Window.partitionBy("tariff", F.month("valid_from_date")).orderBy(F.col("rate").desc(), "id").rowsBetween(-1, Window.currentRow)
w_tariff_valid_from_date_month_2 = Window.partitionBy("tariff", F.month("valid_from_date")).orderBy(F.col("rate").desc())
w_rate_rank_by_valid_from_date = Window.partitionBy("rate_rank").orderBy("valid_from_date", "id")
w_all_by_rate_avg_2_full = Window.orderBy(F.col("rate_avg_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_valid_from_date_month_tariff = Window.partitionBy(F.date_trunc("month", "valid_from_date"), "tariff")
w_valid_from_to_date_tariff = Window.partitionBy(F.to_date("valid_from"), "tariff")
w_rate_rank_by_rate_avg_2 = Window.partitionBy("rate_rank").orderBy(F.col("rate_avg_2").desc(), "id")
w_rate_rank_by_rate_avg_2_runn = Window.partitionBy("rate_rank").orderBy(F.col("rate_avg_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tariffs = (
    df_tariff_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.to_date(F.col("valid_from")).alias("valid_from_date"),
        F.substring(F.trim(F.col("tariff")), 1, 2).alias("tariff_prefix"),
        F.date_format(F.col("valid_from"), "yyyy-MM-dd HH:00").alias("valid_from_label"),
    )
)

sequenced = (
    valid_tariffs
    .select(
        "*",
        F.round(F.avg("rate").over(w_tariff_valid_from_date_month), 2).alias("rate_avg_2"),
        F.rank().over(w_tariff_valid_from_date_month_2).alias("rate_rank"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.row_number().over(w_rate_rank_by_valid_from_date).alias("rn"),
        F.nth_value("rate_rank", 2).over(w_all_by_rate_avg_2_full).alias("second_rate_rank"),
        F.max("rate_rank").over(w_valid_from_date_month_tariff).alias("valid_from_date_month_tariff_max_rate_rank"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
        "valid_from_date",
        "rate_rank",
        "second_rate_rank",
        "valid_from_date_month_tariff_max_rate_rank",
        F.min("rate_rank").over(w_valid_from_to_date_tariff).alias("valid_from_to_date_tariff_min_rate_rank"),
        F.ntile(10).over(w_rate_rank_by_rate_avg_2).alias("rate_avg_2_decile"),
        F.sum("rate").over(w_rate_rank_by_rate_avg_2_runn).alias("running_rate"),
        F.count(F.lit(1)).over(w_valid_from_to_date_tariff).alias("rows_in_valid_from_to_date_tariff"),
        F.row_number().over(w_rate_rank_by_rate_avg_2).alias("rate_avg_2_seq"),
    )
)

result = (
    metrics
    .select("id", "rate", "running_rate")
    .unpivot(
        ids=["id"],
        values=["rate", "running_rate"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
