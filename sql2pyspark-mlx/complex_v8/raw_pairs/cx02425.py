from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_region_by_star = Window.partitionBy("status", "holder_region").orderBy(F.col("start_date").desc(), "id")
w_status_by_end_date_ties = Window.partitionBy("status").orderBy(F.col("end_date").desc())
w_all_by_start_date_running = Window.orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_status_by_end_date = Window.partitionBy("holder_id", "status").orderBy("end_date")
w_all_by_start_date_full = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_segment_by_start_date = Window.partitionBy("holder_segment").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date = Window.orderBy(F.col("start_date").asc_nulls_last(), "id")

base_policys = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2026-01-20").cast("date"))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
    )
)

with_details = (
    base_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_holder_region_by_star)).alias("days_since_prev_start_date"),
        F.dense_rank().over(w_status_by_end_date_ties).alias("end_date_dense_rank"),
        F.first("end_date", ignorenulls=True).over(w_all_by_start_date_running).alias("first_known_end_date"),
        F.rank().over(w_holder_id_status_by_end_date).alias("end_date_rank"),
        F.last("start_date").over(w_all_by_start_date_full).alias("last_start_date"),
        F.first("start_date").over(w_holder_segment_by_start_date).alias("first_start_date"),
        F.rank().over(w_all_by_start_date).alias("start_date_rank"),
        F.round(F.percent_rank().over(w_status_by_end_date_ties), 3).alias("end_date_percent_rank"),
        F.dense_rank().over(w_all_by_start_date).alias("start_date_dense_rank"),
        F.last("start_date", ignorenulls=True).over(w_all_by_start_date_running).alias("last_known_start_date"),
    )
)

result = (
    sequenced
    .filter(F.col("first_known_end_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "holder_segment",
        "holder_region",
        "last_start_date",
        "first_start_date",
        "start_date_rank",
        "end_date_percent_rank",
        "last_known_start_date",
        F.coalesce(F.col("end_date_dense_rank"), F.lit(0)).alias("end_date_dense_rank_filled"),
        F.abs(F.col("end_date_rank") - 6).alias("end_date_rank_distance"),
    )
    .orderBy(F.col("end_date_dense_rank_filled").desc())
)
