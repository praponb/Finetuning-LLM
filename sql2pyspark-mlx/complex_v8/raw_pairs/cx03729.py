from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_score_tier = Window.partitionBy("term", "score_tier")
w_course_id_course_level = Window.partitionBy("course_id", "course_level")
w_student_id_by_enrolled_at_fu = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_score = Window.partitionBy("course_id").orderBy("score", "id")
w_student_id_by_student_gpa = Window.partitionBy("student_id").orderBy(F.col("student_gpa").desc_nulls_first(), "id")
w_student_id_by_enrolled_at_ru = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_by_score_prior_3 = Window.partitionBy("course_id").orderBy("score", "id").rowsBetween(-3, -1)
w_course_id_by_score_moving_1 = Window.partitionBy("course_id").orderBy("score", "id").rowsBetween(-1, Window.currentRow)
w_all_by_student_gpa_ties = Window.orderBy(F.col("student_gpa").desc())
w_student_id_by_student_gpa_fu = Window.partitionBy("student_id").orderBy(F.col("student_gpa").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_enrolled_at_mo = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_course_id_by_score_moving_3 = Window.partitionBy("course_id").orderBy("score", "id").rowsBetween(-3, Window.currentRow)
w_score_tier_by_second_student = Window.partitionBy("score_tier").orderBy(F.col("second_student_gpa").desc())
w_score_tier_student_id = Window.partitionBy("score_tier", "student_id")
w_course_level_second_enrolled = Window.partitionBy("course_level", F.month("second_enrolled_at")).orderBy(F.col("student_gpa_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")

scoped = (
    df_enrollments
    .filter((F.col("term").isin("2025T1", "2024T2", "2024T1")) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.when(F.col("score") >= 74.9, "HIGH").when(F.col("score") >= 38.1, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

joined = (
    scoped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.score_tier",
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
        F.col("c.level").alias("course_level"),
    )
)

sequenced = (
    joined
    .select(
        "*",
        F.sum("score").over(w_term_score_tier).alias("term_score_tier_sum_score"),
        F.round(F.col("score") - F.avg("score").over(w_course_id_course_level), 2).alias("score_vs_course_id_course_level_avg"),
        F.nth_value("student_gpa", 2).over(w_student_id_by_enrolled_at_fu).alias("second_student_gpa"),
        F.dense_rank().over(w_course_id_by_score).alias("score_dense_rank"),
        F.ntile(4).over(w_student_id_by_student_gpa).alias("student_gpa_quartile"),
        F.last("score", ignorenulls=True).over(w_student_id_by_enrolled_at_ru).alias("last_known_score"),
        F.round(F.avg("student_gpa").over(w_course_id_by_score_prior_3), 2).alias("student_gpa_avg_3_prior"),
        F.round(F.avg("score").over(w_course_id_by_score_moving_1), 2).alias("score_avg_2"),
        F.rank().over(w_all_by_student_gpa_ties).alias("student_gpa_rank"),
        F.nth_value("enrolled_at", 2).over(w_student_id_by_student_gpa_fu).alias("second_enrolled_at"),
        F.sum("score").over(w_student_id_by_enrolled_at_mo).alias("score_sum_7"),
        F.round(F.avg("student_gpa").over(w_course_id_by_score_moving_3), 2).alias("student_gpa_avg_4"),
    )
)

leaders = (
    sequenced
    .filter(F.col("score_dense_rank") <= 3)
    .drop("score_dense_rank")
)

windowed = (
    leaders
    .select(
        "*",
        F.dense_rank().over(w_score_tier_by_second_student).alias("second_student_gpa_dense_rank"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_score_tier_student_id), 2).alias("student_gpa_vs_score_tier_student_id_avg"),
        F.max("last_known_score").over(w_course_level_second_enrolled).alias("max_last_known_score_so_far"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
        F.sum("student_gpa_avg_3_prior").over(w_score_tier_student_id).alias("score_tier_student_id_sum_student_gpa_avg_3_prio"),
    )
)

result = (
    windowed
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "score_tier",
        "student_name",
        "student_gpa",
        "course_level",
        "term_score_tier_sum_score",
        "second_student_gpa",
        "student_gpa_quartile",
        "last_known_score",
        "student_gpa_avg_3_prior",
        "score_avg_2",
        "second_enrolled_at",
        "score_sum_7",
        "student_gpa_avg_4",
        "student_gpa_vs_score_tier_student_id_avg",
        "max_last_known_score_so_far",
        "enrolled_at_cume_dist",
        "score_tier_student_id_sum_student_gpa_avg_3_prio",
    )
    .orderBy("student_gpa_vs_score_tier_student_id_avg", F.col("student_gpa").desc())
)
