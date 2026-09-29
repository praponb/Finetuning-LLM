from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_holder_segment_by = Window.partitionBy("holder_id", "holder_segment").orderBy("days_since_start_date", "id")
w_status_holder_region = Window.partitionBy("status", "holder_region")
w_status_by_days_since_start_d = Window.partitionBy("status").orderBy(F.col("days_since_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_holder_region_by_days = Window.partitionBy("status", "holder_region").orderBy("days_since_start_date")
w_status_holder_region_by_days_2 = Window.partitionBy("status", "holder_region").orderBy("days_since_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_days_since_start_d_2 = Window.partitionBy("status").orderBy(F.col("days_since_start_date").desc(), "id")
w_days_since_start_date_dense = Window.partitionBy("days_since_start_date_dense_rank", "holder_segment")
w_holder_segment_holder_id_by = Window.partitionBy("holder_segment", "holder_id").orderBy(F.col("next_days_since_start_date").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_next_days_since_start = Window.orderBy(F.col("next_days_since_start_date").desc())

valid_policys = (
    df_policies
    .filter(F.col("product") == "LIFE")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

with_details = (
    valid_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.days_since_start_date",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.dense_rank().over(w_holder_id_holder_segment_by).alias("days_since_start_date_dense_rank"),
        F.count(F.lit(1)).over(w_status_holder_region).alias("rows_in_status_holder_region"),
        F.last("days_since_start_date").over(w_status_by_days_since_start_d).alias("last_days_since_start_date"),
        F.dense_rank().over(w_status_holder_region_by_days).alias("days_since_start_date_dense_rank_2"),
        F.last("days_since_start_date").over(w_status_holder_region_by_days_2).alias("last_days_since_start_date_2"),
        F.min("holder_risk_score").over(w_status_holder_region).alias("status_holder_region_min_holder_risk_score"),
        F.lag("holder_risk_score", 1, 0).over(w_holder_id_holder_segment_by).alias("prev_holder_risk_score"),
        F.lead("days_since_start_date").over(w_holder_id_holder_segment_by).alias("next_days_since_start_date"),
        F.round(F.percent_rank().over(w_status_by_days_since_start_d_2), 3).alias("days_since_start_date_percent_rank"),
    )
)

top_rows = (
    with_windows
    .filter(F.col("days_since_start_date_dense_rank_2") <= 2)
    .drop("days_since_start_date_dense_rank_2")
)

windowed = (
    top_rows
    .select(
        "id",
        "holder_id",
        "status",
        "days_since_start_date",
        "holder_risk_score",
        "holder_region",
        "days_since_start_date_dense_rank",
        "rows_in_status_holder_region",
        "last_days_since_start_date_2",
        "status_holder_region_min_holder_risk_score",
        "prev_holder_risk_score",
        "days_since_start_date_percent_rank",
        F.round(F.col("days_since_start_date") / F.nullif(F.sum("days_since_start_date").over(w_days_since_start_date_dense), F.lit(0)), 4).alias("days_since_start_date_share_of_days_since_start_"),
        F.size(F.collect_set("status").over(w_days_since_start_date_dense)).alias("distinct_status_in_days_since_start_date_dense_r"),
        F.round(F.avg("prev_holder_risk_score").over(w_holder_segment_holder_id_by), 2).alias("prev_holder_risk_score_avg_7"),
        F.count(F.lit(1)).over(w_days_since_start_date_dense).alias("rows_in_days_since_start_date_dense_rank_holder_"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_days_since_start_date_dense), F.lit(0)), 4).alias("holder_risk_score_share_of_days_since_start_date"),
        F.dense_rank().over(w_all_by_next_days_since_start).alias("next_days_since_start_date_dense_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "holder_id",
        "holder_risk_score",
        "holder_region",
        "days_since_start_date_dense_rank",
        "rows_in_status_holder_region",
        "status_holder_region_min_holder_risk_score",
        "prev_holder_risk_score",
        "days_since_start_date_share_of_days_since_start_",
        "prev_holder_risk_score_avg_7",
        "holder_risk_score_share_of_days_since_start_date",
        "next_days_since_start_date_dense_rank",
    )
    .orderBy(F.col("prev_holder_risk_score").asc_nulls_last(), F.col("id").desc())
    .limit(5)
)
