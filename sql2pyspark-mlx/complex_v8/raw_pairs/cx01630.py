from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_row_count_ties = Window.orderBy(F.col("row_count").desc())
w_row_count_by_row_count = Window.partitionBy("row_count").orderBy(F.col("row_count").desc(), "region", "joined_on_ym")
w_first_joined_on_year_region = Window.partitionBy(F.year("first_joined_on"), "region").orderBy("row_count", "region", "joined_on_ym")
w_all_by_row_count_running = Window.orderBy(F.col("row_count").desc(), "region", "joined_on_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_row_count_by_row_count_runni = Window.partitionBy("row_count").orderBy(F.col("row_count").desc(), "region", "joined_on_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_joined_on_month_by_max = Window.partitionBy(F.month("first_joined_on")).orderBy("max_risk_score", "region", "joined_on_ym")
w_row_count = Window.partitionBy("row_count")
w_first_joined_on_month_by_max_2 = Window.partitionBy(F.month("first_joined_on")).orderBy("max_risk_score", "region", "joined_on_ym").rowsBetween(-1, Window.currentRow)
w_all_by_row_count_tercile_2_t = Window.orderBy("row_count_tercile_2")
w_row_count_tercile_2_dense_ra = Window.partitionBy("row_count_tercile_2_dense_rank")
w_row_count_tercile_2_dense_ra_2 = Window.partitionBy("row_count_tercile_2_dense_rank").orderBy(F.col("row_count_tercile").asc_nulls_last(), "region", "joined_on_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_row_count_tercile_2_dense_ra_3 = Window.partitionBy("row_count_tercile_2_dense_rank").orderBy(F.col("row_count_tercile").asc_nulls_last(), "region", "joined_on_ym")

recent = (
    df_policyholders
    .filter((F.col("segment") != "RETAIL") & (F.year(F.col("joined_on")) == 2024))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
    )
)

screened = (
    recent.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.year(F.col("p.end_date")) == 2026) & (F.col("p.premium") <= 3719.7), "left_anti")
)

totals = (
    screened
    .groupBy("region", F.date_format("joined_on", "yyyy-MM").alias("joined_on_ym"))
    .agg(
        F.max("risk_score").alias("max_risk_score"),
        F.min("joined_on").alias("first_joined_on"),
        F.round(F.sum("risk_score_filled"), 2).alias("total_risk_score_filled"),
        F.count("*").alias("row_count"),
    )
)

sequenced = (
    totals
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_row_count_ties), 3).alias("row_count_percent_rank"),
        F.ntile(3).over(w_row_count_by_row_count).alias("row_count_tercile"),
        F.ntile(3).over(w_first_joined_on_year_region).alias("row_count_tercile_2"),
        F.first("max_risk_score", ignorenulls=True).over(w_all_by_row_count_running).alias("first_known_max_risk_score"),
        F.sum("total_risk_score_filled").over(w_row_count_by_row_count_runni).alias("running_total_risk_score_filled"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.round(F.percent_rank().over(w_first_joined_on_month_by_max), 3).alias("max_risk_score_percent_rank"),
        F.round(F.avg("row_count_tercile_2").over(w_row_count), 2).alias("row_count_avg_row_count_tercile_2"),
        F.max("total_risk_score_filled").over(w_first_joined_on_month_by_max_2).alias("total_risk_score_filled_max_2"),
        F.round(F.col("row_count_percent_rank") / F.nullif(F.sum("row_count_percent_rank").over(w_row_count), F.lit(0)), 4).alias("row_count_percent_rank_share_of_row_count"),
        F.dense_rank().over(w_all_by_row_count_tercile_2_t).alias("row_count_tercile_2_dense_rank"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "region",
        "joined_on_ym",
        "max_risk_score",
        "first_joined_on",
        "row_count",
        "row_count_percent_rank",
        "first_known_max_risk_score",
        "row_count_tercile_2_dense_rank",
        F.round(F.avg("row_count_percent_rank").over(w_row_count_tercile_2_dense_ra), 2).alias("row_count_tercile_2_dense_rank_avg_row_count_per"),
        F.first("row_count_tercile").over(w_row_count_tercile_2_dense_ra_2).alias("first_row_count_tercile"),
        (F.col("max_risk_score_percent_rank") - F.lag("max_risk_score_percent_rank").over(w_row_count_tercile_2_dense_ra_3)).alias("max_risk_score_percent_rank_change"),
    )
)

filtered = (
    with_windows_2
    .crossJoin(with_windows_2.agg(F.max("row_count_percent_rank").alias("overall_max_row_count_percent_rank")))
    .filter(F.col("row_count_percent_rank") < F.col("overall_max_row_count_percent_rank"))
    .drop("overall_max_row_count_percent_rank")
)

result = (
    filtered
    .select(
        "region",
        "joined_on_ym",
        "max_risk_score",
        "first_joined_on",
        "row_count",
        "row_count_percent_rank",
        "first_known_max_risk_score",
        "row_count_tercile_2_dense_rank",
        "first_row_count_tercile",
        "max_risk_score_percent_rank_change",
        F.lower(F.trim(F.col("region"))).alias("region_clean"),
    )
    .orderBy(
        "max_risk_score",
        F.col("first_row_count_tercile").desc_nulls_first(),
        "region",
        "joined_on_ym",
    )
    .limit(10)
)
