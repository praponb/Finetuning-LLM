from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy("score", "id")
w_course_level_grade_status = Window.partitionBy("course_level", "grade_status")
w_course_id_student_id_by_scor = Window.partitionBy("course_id", "student_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_student_id_by_scor_2 = Window.partitionBy("course_id", "student_id").orderBy(F.col("score").desc(), "id")
w_all_by_score_running = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_score = Window.partitionBy("term").orderBy(F.col("score").asc_nulls_last(), "id")
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_term_by_course_level_grade_s = Window.partitionBy("term").orderBy(F.col("course_level_grade_status_min_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_next3_score_per_next = Window.orderBy("next3_score_per_next_score", "id")
w_term_by_course_level_grade_s_2 = Window.partitionBy("term").orderBy(F.col("course_level_grade_status_min_score").desc(), "id")

recent = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) | (F.col("score") <= 85.2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

joined = (
    recent.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (~F.col("st.is_international")), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select("s.*", "st.student_name", F.col("c.level").alias("course_level"))
)

with_windows = (
    joined
    .select(
        "*",
        F.lead("score").over(w_all_by_score).alias("next_score"),
        F.min("score").over(w_course_level_grade_status).alias("course_level_grade_status_min_score"),
        F.nth_value("score", 2).over(w_course_id_student_id_by_scor).alias("second_score"),
        F.round(F.col("score") - F.avg("score").over(w_course_level_grade_status), 2).alias("score_vs_course_level_grade_status_avg"),
        F.round(F.cume_dist().over(w_course_id_student_id_by_scor_2), 3).alias("score_cume_dist"),
        F.first("score", ignorenulls=True).over(w_all_by_score_running).alias("first_known_score"),
        F.lead("score", 3).over(w_term_by_score).alias("next3_score"),
        (F.col("score") - F.lag("score").over(w_term_by_score)).alias("score_change"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_level_grade_status), F.lit(0)), 4).alias("score_share_of_course_level_grade_status"),
        F.sum("score").over(w_student_id_grade_status).alias("student_id_grade_status_sum_score"),
    )
)

tagged = (
    with_windows
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at_label",
        "student_name",
        "course_level",
        "next_score",
        "course_level_grade_status_min_score",
        "second_score",
        "score_vs_course_level_grade_status_avg",
        "first_known_score",
        "next3_score",
        "score_share_of_course_level_grade_status",
        "student_id_grade_status_sum_score",
        (F.col("score_change") - F.col("score")).alias("score_change_minus_score"),
        F.when(F.col("score_cume_dist") >= 0.8, "A").when(F.col("score_cume_dist") >= 0.4, "B").otherwise("C").alias("score_cume_dist_tier"),
        F.round(F.col("next3_score") / F.nullif(F.col("next_score"), F.lit(0)), 3).alias("next3_score_per_next_score"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.sum("second_score").over(w_term_by_course_level_grade_s).alias("running_second_score"),
        (F.col("first_known_score") - F.lag("first_known_score").over(w_all_by_next3_score_per_next)).alias("first_known_score_change"),
        F.lag("next3_score").over(w_term_by_course_level_grade_s_2).alias("prev_next3_score"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at_label",
        "course_level",
        "course_level_grade_status_min_score",
        "second_score",
        "score_vs_course_level_grade_status_avg",
        "first_known_score",
        "next3_score",
        "score_share_of_course_level_grade_status",
        "student_id_grade_status_sum_score",
        "score_change_minus_score",
        "score_cume_dist_tier",
        "running_second_score",
        "first_known_score_change",
        "prev_next3_score",
    )
    .orderBy(
        F.col("score_change_minus_score").desc(),
        F.col("running_second_score").desc(),
    )
)
