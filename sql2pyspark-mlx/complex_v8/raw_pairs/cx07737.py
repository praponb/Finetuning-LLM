from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_enrolled_at_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_enrolled_at_full = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_enrolled_at_ties = Window.partitionBy("term").orderBy("enrolled_at")
w_row_count_enrolled_at_dense = Window.partitionBy("row_count", "enrolled_at_dense_rank")
w_enrolled_at_dense_rank_row_c = Window.partitionBy("enrolled_at_dense_rank", "row_count").orderBy(F.col("total_enrolled_at_dense_rank").desc(), "enrolled_at_dense_rank", "first_known_enrolled_at_month", "last_enrolled_at_month")
w_row_count_enrolled_at_dense_2 = Window.partitionBy("row_count", "enrolled_at_dense_rank").orderBy(F.col("total_enrolled_at_dense_rank").asc_nulls_last(), "enrolled_at_dense_rank", "first_known_enrolled_at_month", "last_enrolled_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_enrollments
    .filter((F.col("term") == "2025T1") & (F.col("score") < 66.0))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

derived = (
    scoped
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "course_id",
        "term",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

sequenced = (
    derived
    .select(
        "id",
        F.first("enrolled_at", ignorenulls=True).over(w_term_by_enrolled_at_running).alias("first_known_enrolled_at"),
        F.last("enrolled_at").over(w_term_by_enrolled_at_full).alias("last_enrolled_at"),
        F.dense_rank().over(w_term_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
    )
)

totals = (
    sequenced
    .filter(F.col("enrolled_at_dense_rank") > 2)
    .groupBy(
        "enrolled_at_dense_rank",
        F.date_trunc("month", "first_known_enrolled_at").alias("first_known_enrolled_at_month"),
        F.date_trunc("month", "last_enrolled_at").alias("last_enrolled_at_month"),
    )
    .agg(
        F.round(F.avg("enrolled_at_dense_rank"), 2).alias("avg_enrolled_at_dense_rank"),
        F.count("*").alias("row_count"),
        F.sum("enrolled_at_dense_rank").alias("total_enrolled_at_dense_rank"),
    )
)

with_windows = (
    totals
    .select(
        "enrolled_at_dense_rank",
        "first_known_enrolled_at_month",
        "last_enrolled_at_month",
        "avg_enrolled_at_dense_rank",
        F.round(F.col("total_enrolled_at_dense_rank") - F.avg("total_enrolled_at_dense_rank").over(w_row_count_enrolled_at_dense), 2).alias("total_enrolled_at_dense_rank_vs_row_count_enroll"),
        F.dense_rank().over(w_enrolled_at_dense_rank_row_c).alias("total_enrolled_at_dense_rank_dense_rank"),
        F.round(F.avg("total_enrolled_at_dense_rank").over(w_row_count_enrolled_at_dense), 2).alias("row_count_enrolled_at_dense_rank_avg_total_enrol"),
        F.max("avg_enrolled_at_dense_rank").over(w_row_count_enrolled_at_dense).alias("row_count_enrolled_at_dense_rank_max_avg_enrolle"),
        F.max("avg_enrolled_at_dense_rank").over(w_row_count_enrolled_at_dense_2).alias("max_avg_enrolled_at_dense_rank_so_far"),
    )
)

result = (
    with_windows
    .filter(F.col("first_known_enrolled_at_month") < F.lit("2025-02-17 09:00:00").cast("timestamp"))
    .select(
        "enrolled_at_dense_rank",
        "first_known_enrolled_at_month",
        "last_enrolled_at_month",
        "total_enrolled_at_dense_rank_vs_row_count_enroll",
        "total_enrolled_at_dense_rank_dense_rank",
        "row_count_enrolled_at_dense_rank_avg_total_enrol",
        F.month(F.col("last_enrolled_at_month")).alias("last_enrolled_at_month_month"),
        F.abs(F.col("enrolled_at_dense_rank") - 6).alias("enrolled_at_dense_rank_distance"),
    )
    .orderBy(
        F.col("last_enrolled_at_month").desc_nulls_first(),
        F.col("enrolled_at_dense_rank").asc(),
    )
)
