from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_enrolled_at_full = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_enrolled_at_moving_2 = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(-2, Window.currentRow)
w_term = Window.partitionBy("term")
w_term_by_enrolled_at_prior_1 = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(-1, -1)
w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_by_enrolled_at_ties = Window.partitionBy("term").orderBy("enrolled_at")
w_all_by_prev_enrolled_at_runn = Window.orderBy(F.col("prev_enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_prev_enrolled_at = Window.orderBy("prev_enrolled_at", "id")
w_all_by_prev_enrolled_at_runn_2 = Window.orderBy("prev_enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_enrollments = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 57.3).alias("score_distance"),
        F.abs(F.col("score") - 38.1).alias("score_distance_2"),
    )
)

joined = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select("s.*", "st.student_name", F.col("st.gpa").alias("student_gpa"))
)

with_trends = (
    joined
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "student_name",
        "student_gpa",
        F.last("score_distance_2").over(w_term_by_enrolled_at_full).alias("last_score_distance_2"),
        F.round(F.avg("score_distance_2").over(w_term_by_enrolled_at_moving_2), 2).alias("score_distance_2_avg_3"),
        F.round(F.col("score_distance") - F.avg("score_distance").over(w_term), 2).alias("score_distance_vs_term_avg"),
        F.round(F.col("score_distance_2") / F.nullif(F.sum("score_distance_2").over(w_term), F.lit(0)), 4).alias("score_distance_2_share_of_term"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.sum("score_distance").over(w_term_by_enrolled_at_prior_1).alias("score_distance_sum_1_prior"),
        F.lag("enrolled_at").over(w_term_by_enrolled_at).alias("prev_enrolled_at"),
        F.dense_rank().over(w_term_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
    )
)

top_rows = (
    with_trends
    .filter((F.col("enrolled_at_dense_rank") == 1) & (F.col("last_score_distance_2") >= 30.0))
    .drop("enrolled_at_dense_rank")
)

windowed = (
    top_rows
    .select(
        "*",
        F.first("score_distance_vs_term_avg", ignorenulls=True).over(w_all_by_prev_enrolled_at_runn).alias("first_known_score_distance_vs_term_avg"),
        F.round(F.cume_dist().over(w_all_by_prev_enrolled_at), 3).alias("prev_enrolled_at_cume_dist"),
        F.sum("score_distance_vs_term_avg").over(w_all_by_prev_enrolled_at_runn_2).alias("running_score_distance_vs_term_avg"),
        F.round(F.col("score_distance") - F.avg("score_distance").over(w_term), 2).alias("score_distance_vs_term_avg_2"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "student_name",
        "last_score_distance_2",
        "score_distance_2_avg_3",
        "score_distance_vs_term_avg",
        "score_distance_2_share_of_term",
        "rows_in_term",
        "score_distance_sum_1_prior",
        "prev_enrolled_at",
        "prev_enrolled_at_cume_dist",
        "running_score_distance_vs_term_avg",
        "score_distance_vs_term_avg_2",
        "rows_in_term_2",
    )
)
