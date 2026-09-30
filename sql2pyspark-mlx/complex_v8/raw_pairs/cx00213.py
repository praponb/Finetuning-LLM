from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_score_ties = Window.partitionBy("grade_status").orderBy(F.col("score").desc_nulls_first())
w_course_id_grade_status_group = Window.partitionBy("course_id", "grade_status_group")
w_all_by_score_ties = Window.orderBy(F.col("score").desc())
w_grade_status_by_score_runnin = Window.partitionBy("grade_status").orderBy(F.col("score").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_score_rank_2 = Window.partitionBy("score_rank_2")
w_score_rank_2_by_score_dense = Window.partitionBy("score_rank_2").orderBy(F.col("score_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_score_rank_2_f = Window.partitionBy("student_id").orderBy(F.col("score_rank_2").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_score_rank_2_by_score_dense_2 = Window.partitionBy("score_rank_2").orderBy(F.col("score_dense_rank").desc(), "id")
w_course_id_last_score_rank = Window.partitionBy("course_id", "last_score_rank")
w_all_by_last_score_rank = Window.orderBy("last_score_rank", "id")
w_all_by_last_score_rank_full = Window.orderBy("last_score_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_enrollments
    .filter(F.col("score").between(34.2, 81.4))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("WITHDRAWN", "FAIL"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

with_activity = (
    base.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status").isin("INCOMPLETE", "FAIL")) & (F.col("e.score").between(34.2, 94.7)), "left_anti")
)

with_trends = (
    with_activity
    .select(
        "*",
        F.rank().over(w_grade_status_by_score_ties).alias("score_rank"),
        F.round(F.avg("days_since_enrolled_at").over(w_course_id_grade_status_group), 2).alias("course_id_grade_status_group_avg_days_since_enro"),
        F.rank().over(w_all_by_score_ties).alias("score_rank_2"),
        F.dense_rank().over(w_all_by_score_ties).alias("score_dense_rank"),
        F.first("score", ignorenulls=True).over(w_grade_status_by_score_runnin).alias("first_known_score"),
        F.row_number().over(w_all_by_score).alias("seq_no"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at",
        "course_id_grade_status_group_avg_days_since_enro",
        "score_dense_rank",
        "seq_no",
        F.min("score_rank").over(w_score_rank_2).alias("score_rank_2_min_score_rank"),
        F.last("score_rank").over(w_score_rank_2_by_score_dense).alias("last_score_rank"),
        F.first("score").over(w_student_id_by_score_rank_2_f).alias("first_score"),
        F.max("days_since_enrolled_at").over(w_score_rank_2).alias("score_rank_2_max_days_since_enrolled_at"),
        F.rank().over(w_score_rank_2_by_score_dense_2).alias("score_dense_rank_rank"),
    )
)

ranked = (
    sequenced
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at",
        "course_id_grade_status_group_avg_days_since_enro",
        "score_dense_rank",
        "seq_no",
        "score_rank_2_min_score_rank",
        "last_score_rank",
        "score_dense_rank_rank",
        F.round(F.col("seq_no") - F.avg("seq_no").over(w_course_id_last_score_rank), 2).alias("seq_no_vs_course_id_last_score_rank_avg"),
        F.rank().over(w_all_by_last_score_rank).alias("last_score_rank_rank"),
        F.nth_value("first_score", 2).over(w_all_by_last_score_rank_full).alias("second_first_score"),
    )
)

annotated = (
    ranked
    .select(
        "*",
        (F.col("score_dense_rank_rank") + F.col("last_score_rank_rank")).alias("score_dense_rank_rank_plus_last_score_rank_rank"),
        (F.col("score_dense_rank") + F.col("second_first_score")).alias("score_dense_rank_plus_second_first_score"),
    )
)

result = (
    annotated
    .filter(F.col("student_id") >= 14)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at",
        "course_id_grade_status_group_avg_days_since_enro",
        "score_dense_rank",
        "seq_no",
        "score_rank_2_min_score_rank",
        "last_score_rank",
        "score_dense_rank_rank",
        "seq_no_vs_course_id_last_score_rank_avg",
        "second_first_score",
    )
    .orderBy("seq_no_vs_course_id_last_score_rank_avg")
)
