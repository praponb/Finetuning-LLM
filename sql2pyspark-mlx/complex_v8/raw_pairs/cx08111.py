from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_enrolled_on_year_by = Window.partitionBy(F.year("student_enrolled_on")).orderBy(F.col("student_gpa").desc_nulls_first(), "id")
w_enrolled_at_month_student_id = Window.partitionBy(F.month("enrolled_at"), "student_id")
w_student_enrolled_on_year_by_2 = Window.partitionBy(F.year("student_enrolled_on")).orderBy(F.col("student_gpa").desc_nulls_first())
w_student_gpa_rank_student_enr = Window.partitionBy("student_gpa_rank", F.month("student_enrolled_on"))
w_all_by_distinct_course_id_in = Window.orderBy("distinct_course_id_in_enrolled_at_month_student_", "id")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 11)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 73.7, "HIGH").when(F.col("score") >= 42.4, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
    )
)

joined_student = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        F.col("st.gpa").alias("student_gpa"),
        "st.student_name",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        "c.course_code",
    )
)

ranked = (
    joined_student
    .select(
        "*",
        F.ntile(5).over(w_student_enrolled_on_year_by).alias("student_gpa_quintile"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_month_student_id)).alias("distinct_course_id_in_enrolled_at_month_student_"),
        F.count(F.lit(1)).over(w_enrolled_at_month_student_id).alias("rows_in_enrolled_at_month_student_id"),
        F.rank().over(w_student_enrolled_on_year_by_2).alias("student_gpa_rank"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_enrolled_on_year_by))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "student_enrolled_on",
        "student_gpa_quintile",
        "distinct_course_id_in_enrolled_at_month_student_",
        "student_gpa_rank",
        F.round(F.col("student_gpa_rank") - F.avg("student_gpa_rank").over(w_student_gpa_rank_student_enr), 2).alias("student_gpa_rank_vs_student_gpa_rank_student_enr"),
        F.row_number().over(w_all_by_distinct_course_id_in).alias("distinct_course_id_in_enrolled_at_month_student__2"),
        F.lag("student_enrolled_on").over(w_all_by_distinct_course_id_in).alias("prev_student_enrolled_on"),
    )
)

with_children_enrollments = (
    sequenced.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.student_enrolled_on",
        "s.student_gpa_quintile",
        "s.distinct_course_id_in_enrolled_at_month_student_",
        "s.student_gpa_rank",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    with_children_enrollments
    .rollup("enrollment_term", "distinct_course_id_in_enrolled_at_month_student_")
    .agg(
        F.percentile_approx("enrollment_score", 0.5).alias("median_enrollment_score"),
        F.sum(F.when(F.col("enrollment_term") == "2024T1", 1).otherwise(0)).alias("n_enrollment_term_2024t1"),
        F.sum("distinct_course_id_in_enrolled_at_month_student_").alias("total_distinct_course_id_in_enrolled_at_month_st"),
    )
    .orderBy(
        F.col("enrollment_term").asc_nulls_last(),
        F.col("distinct_course_id_in_enrolled_at_month_student_").asc_nulls_last(),
    )
)
