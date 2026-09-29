from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_score = Window.partitionBy("student_id").orderBy("score", "id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_all_by_score = Window.orderBy("score", "id")
w_student_id_by_score_full = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_running = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_student_id_by_s = Window.partitionBy("grade_status", "student_id").orderBy("score", "id")
w_student_id_grade_status_by_s = Window.partitionBy("student_id", "grade_status").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_course_level = Window.partitionBy("grade_status", "course_level")
w_grade_status_student_id_by_s_2 = Window.partitionBy("grade_status", "student_id").orderBy("score")
w_course_level = Window.partitionBy("course_level")
w_score_dense_rank = Window.partitionBy("score_dense_rank")
w_enrolled_at_seq_by_score_ful = Window.partitionBy("enrolled_at_seq").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_course_level_by_e = Window.partitionBy("student_id", "course_level").orderBy("enrolled_at", "id")
w_student_id_course_level_by_e_2 = Window.partitionBy("student_id", "course_level").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_seq_by_score_tie = Window.partitionBy("enrolled_at_seq").orderBy(F.col("score").desc())

scoped = (
    df_enrollments
    .filter((F.month(F.col("enrolled_at")).isin(1, 2)) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.level") == "advanced")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_slug",
        F.col("c.level").alias("course_level"),
    )
)

with_windows = (
    with_parent
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "course_level",
        F.rank().over(w_student_id_by_score).alias("score_rank"),
        F.row_number().over(w_all_by_enrolled_at).alias("enrolled_at_seq"),
        F.sum("score").over(w_grade_status_course_id).alias("grade_status_course_id_sum_score"),
        F.ntile(4).over(w_all_by_score).alias("score_quartile"),
        F.first("enrolled_at").over(w_student_id_by_score_full).alias("first_enrolled_at"),
        F.first("score", ignorenulls=True).over(w_all_by_score_running).alias("first_known_score"),
        F.min("score").over(w_grade_status_course_id).alias("grade_status_course_id_min_score"),
        F.lead("score").over(w_grade_status_student_id_by_s).alias("next_score"),
        F.count(F.lit(1)).over(w_student_id_grade_status_by_s).alias("running_count"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_course_level), 2).alias("score_vs_grade_status_course_level_avg"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_course_level), F.lit(0)), 4).alias("score_share_of_grade_status_course_level"),
        F.dense_rank().over(w_grade_status_student_id_by_s_2).alias("score_dense_rank"),
        F.lag("score", 2).over(w_all_by_score).alias("prev2_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_level), F.lit(0)), 4).alias("score_share_of_course_level"),
    )
)

scored = (
    with_windows
    .filter(F.col("student_id") <= 4)
    .select(
        "*",
        F.concat(F.col("course_level"), F.lit("#"), F.col("enrolled_at_seq").cast("string")).alias("course_level_ref"),
    )
)

windowed = (
    scored
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_seq",
        "score_quartile",
        "first_known_score",
        "grade_status_course_id_min_score",
        "next_score",
        "running_count",
        "score_vs_grade_status_course_level_avg",
        "score_share_of_grade_status_course_level",
        "prev2_score",
        "score_share_of_course_level",
        "course_level_ref",
        F.round(F.col("first_known_score") - F.avg("first_known_score").over(w_score_dense_rank), 2).alias("first_known_score_vs_score_dense_rank_avg"),
        F.size(F.collect_set("grade_status").over(w_score_dense_rank)).alias("distinct_grade_status_in_score_dense_rank"),
        F.last("score_dense_rank").over(w_enrolled_at_seq_by_score_ful).alias("last_score_dense_rank"),
        F.row_number().over(w_student_id_course_level_by_e).alias("rn"),
        F.sum("enrolled_at_seq").over(w_student_id_course_level_by_e_2).alias("running_enrolled_at_seq"),
        F.nth_value("score", 2).over(w_enrolled_at_seq_by_score_ful).alias("second_score"),
        F.dense_rank().over(w_enrolled_at_seq_by_score_tie).alias("score_dense_rank_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_quartile",
        "first_known_score",
        "next_score",
        "running_count",
        "score_vs_grade_status_course_level_avg",
        "score_share_of_grade_status_course_level",
        "prev2_score",
        "score_share_of_course_level",
        "course_level_ref",
        "distinct_grade_status_in_score_dense_rank",
        "running_enrolled_at_seq",
        "second_score",
        F.coalesce(F.col("score_dense_rank_2"), F.lit(0)).alias("score_dense_rank_2_filled"),
        F.round(F.col("score_dense_rank_2") / F.nullif(F.col("prev2_score"), F.lit(0)), 3).alias("score_dense_rank_2_per_prev2_score"),
    )
    .orderBy(
        F.col("score_vs_grade_status_course_level_avg").desc_nulls_first(),
        "score_dense_rank_2_filled",
    )
)
