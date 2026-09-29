from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_course_credits_by = Window.partitionBy("course_id", "course_credits").orderBy(F.col("enrolled_at").desc())
w_course_credits_by_score = Window.partitionBy("course_credits").orderBy("score", "id")
w_course_credits_by_score_full = Window.partitionBy("course_credits").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_credits_by_score_movi = Window.partitionBy("course_credits").orderBy("score", "id").rowsBetween(-3, Window.currentRow)
w_course_credits_by_score_runn = Window.partitionBy("course_credits").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_term = Window.partitionBy("student_id", "term")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("score") >= 8.7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_parent = (
    scoped_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.length(F.trim(F.col("c.course_code"))) >= 5), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("c.credits") >= 2)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        F.col("c.credits").alias("course_credits"),
        "st.student_name",
    )
)

sequenced = (
    with_parent
    .select(
        "*",
        F.dense_rank().over(w_course_id_course_credits_by).alias("enrolled_at_dense_rank"),
        F.lead("course_credits").over(w_course_credits_by_score).alias("next_course_credits"),
        F.nth_value("score", 2).over(w_course_credits_by_score_full).alias("second_score"),
        F.round(F.avg("course_credits").over(w_course_credits_by_score_movi), 2).alias("course_credits_avg_4"),
        F.first("score", ignorenulls=True).over(w_course_credits_by_score_runn).alias("first_known_score"),
        F.sum("score").over(w_student_id_term).alias("student_id_term_sum_score"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("enrollment_term_count"),
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
    )
)

summarized_rows = (
    sequenced.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

screened = (
    summarized_rows.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.upper(F.trim(F.col("c.course_code"))).like("M%")) & (F.col("c.level") == "intro"), "left_semi")
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

with_counts = (
    screened.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score",
        "e.avg_enrollment_score",
        "e.last_enrollment_enrolled_at",
    )
)

result = (
    with_counts
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "course_credits",
        "student_name",
        "enrolled_at_dense_rank",
        "next_course_credits",
        "course_credits_avg_4",
        "student_id_term_sum_score",
        "enrollment_count",
        "enrollment_count_2",
        "total_enrollment_score",
        "avg_enrollment_score",
        "last_enrollment_enrolled_at",
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
    )
    .orderBy(
        F.col("score").desc_nulls_first(),
        F.col("student_id_term_sum_score").desc(),
    )
)
