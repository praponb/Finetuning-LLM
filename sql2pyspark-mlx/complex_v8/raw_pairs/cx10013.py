from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status")
w_student_id = Window.partitionBy("student_id")
w_all_by_enrolled_at_hour_ties = Window.orderBy(F.col("enrolled_at_hour").desc())
w_all_by_student_gpa = Window.orderBy(F.col("student_gpa").desc(), "id")
w_course_id_course_level_by_sc = Window.partitionBy("course_id", "course_level").orderBy(F.col("score").asc_nulls_last())
w_course_id_course_level_by_sc_2 = Window.partitionBy("course_id", "course_level").orderBy(F.col("score").asc_nulls_last(), "id")
w_course_id_course_level_by_sc_3 = Window.partitionBy("course_id", "course_level").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_course_level_by_sc_4 = Window.partitionBy("course_id", "course_level").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_student_gpa_full = Window.orderBy(F.col("student_gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_hour_by_enrolled = Window.partitionBy("enrolled_at_hour").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_enrollments = (
    df_enrollments
    .filter((F.col("score") >= 32.7) & (F.col("course_id") <= 8))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

with_details_course = (
    scoped_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.col("c.level").isNotNull())
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_hour",
        F.col("c.level").alias("course_level"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

sequenced = (
    with_details_course
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_hour",
        "course_level",
        F.size(F.collect_set("course_level").over(w_grade_status)).alias("distinct_course_level_in_grade_status"),
        F.min("student_gpa").over(w_student_id).alias("student_id_min_student_gpa"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_hour_ties), 3).alias("enrolled_at_hour_cume_dist"),
        F.round(F.col("enrolled_at_hour") - F.avg("enrolled_at_hour").over(w_student_id), 2).alias("enrolled_at_hour_vs_student_id_avg"),
        F.round(F.percent_rank().over(w_all_by_student_gpa), 3).alias("student_gpa_percent_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.rank().over(w_course_id_course_level_by_sc).alias("score_rank"),
        F.rank().over(w_course_id_course_level_by_sc_2).alias("score_rank_2"),
        F.nth_value("enrolled_at", 2).over(w_course_id_course_level_by_sc_3).alias("second_enrolled_at"),
        F.max("score").over(w_course_id_course_level_by_sc_4).alias("max_score_so_far"),
        F.nth_value("enrolled_at", 2).over(w_all_by_student_gpa_full).alias("second_enrolled_at_2"),
        F.min("enrolled_at_hour").over(w_enrolled_at_hour_by_enrolled).alias("min_enrolled_at_hour_so_far"),
    )
)

labeled = (
    sequenced
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "course_level",
        "student_id_min_student_gpa",
        "student_gpa_percent_rank",
        "score_rank_2",
        "max_score_so_far",
        "second_enrolled_at_2",
        F.round(F.col("student_gpa_percent_rank") * 1.5, 2).alias("student_gpa_percent_rank_adj"),
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

result = (
    labeled
    .filter(F.col("max_score_so_far") < 600.0)
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "course_level",
        "student_id_min_student_gpa",
        "score_rank_2",
        "max_score_so_far",
        "second_enrolled_at_2",
        "grade_status_clean",
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean_2"),
    )
    .orderBy(F.col("second_enrolled_at_2").desc())
)
