from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id = Window.partitionBy("course_id")
w_course_id_by_score = Window.partitionBy("course_id").orderBy("score", "id")

scoped_enrollments = (
    df_enrollments
    .filter((F.col("course_id") >= 3) | (F.col("enrolled_at") < F.lit("2025-02-18 08:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
)

windowed = (
    scoped_enrollments
    .select(
        "*",
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.last("score").over(w_course_id_grade_status_by_en).alias("last_score"),
    )
)

tagged = (
    windowed
    .filter(F.col("score") > 20.2)
    .select(
        "*",
        F.round(F.col("enrolled_at_percent_rank") * 1.07, 2).alias("enrolled_at_percent_rank_adj"),
        (F.col("enrolled_at_rank") - F.col("score")).alias("enrolled_at_rank_minus_score"),
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

with_parent_course = (
    tagged.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_label",
        "s.enrolled_at_rank",
        "s.enrolled_at_percent_rank",
        "s.enrolled_at_percent_rank_adj",
        "s.enrolled_at_rank_minus_score",
        "s.enrolled_at_year",
        "c.course_code",
        F.col("st.cohort").alias("student_cohort"),
    )
)

sequenced = (
    with_parent_course
    .select(
        "id",
        "grade_status",
        "enrolled_at_label",
        "enrolled_at_rank",
        "enrolled_at_percent_rank",
        "enrolled_at_percent_rank_adj",
        "enrolled_at_rank_minus_score",
        "enrolled_at_year",
        "course_code",
        "student_cohort",
        F.size(F.collect_set("student_id").over(w_course_id)).alias("distinct_student_id_in_course_id"),
        (F.col("score") - F.lag("score").over(w_course_id_by_score)).alias("score_change"),
    )
)

result = (
    sequenced
    .filter(F.col("enrolled_at_rank") >= 5)
    .select(
        "id",
        "grade_status",
        "enrolled_at_label",
        "enrolled_at_rank",
        "enrolled_at_percent_rank_adj",
        "enrolled_at_year",
        "course_code",
        "student_cohort",
        "distinct_student_id_in_course_id",
        "score_change",
    )
    .orderBy(F.col("course_code").desc_nulls_first())
)
