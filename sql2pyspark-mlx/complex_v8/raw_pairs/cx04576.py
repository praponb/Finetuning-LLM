from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_student_id_by_stud = Window.partitionBy("course_id", "student_id").orderBy("student_gpa")
w_grade_status_student_id_by_e = Window.partitionBy("grade_status", "student_id").orderBy("enrolled_at", "id")
w_enrolled_at_month_student_id = Window.partitionBy(F.month("enrolled_at"), "student_id")
w_all_by_student_gpa = Window.orderBy(F.col("student_gpa").desc(), "id")
w_course_id_student_id_by_stud_2 = Window.partitionBy("course_id", "student_id").orderBy("student_gpa", "id").rowsBetween(-2, -1)
w_grade_status_student_id_by_e_2 = Window.partitionBy("grade_status", "student_id").orderBy("enrolled_at")
w_enrolled_at_quarter = Window.partitionBy("enrolled_at_quarter")
w_enrolled_at_quarter_by_next = Window.partitionBy("enrolled_at_quarter").orderBy("next_student_gpa", "id")
w_all_by_distinct_course_id_in = Window.orderBy("distinct_course_id_in_enrolled_at_month_student_")
w_enrolled_at_quarter_by_next_2 = Window.partitionBy("enrolled_at_quarter").orderBy("next_student_gpa", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_rank_course_id = Window.partitionBy("enrolled_at_rank", "course_id")

filtered_enrollments = df_enrollments.filter((F.col("student_id") <= 11) | (F.col("score") >= 36.6))

joined = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.rank().over(w_course_id_student_id_by_stud).alias("student_gpa_rank"),
        F.ntile(10).over(w_grade_status_student_id_by_e).alias("enrolled_at_decile"),
        F.count(F.lit(1)).over(w_enrolled_at_month_student_id).alias("rows_in_enrolled_at_month_student_id"),
        F.lead("student_gpa").over(w_all_by_student_gpa).alias("next_student_gpa"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_month_student_id)).alias("distinct_course_id_in_enrolled_at_month_student_"),
        F.round(F.avg("student_gpa").over(w_course_id_student_id_by_stud_2), 2).alias("student_gpa_avg_2_prior"),
        F.rank().over(w_grade_status_student_id_by_e_2).alias("enrolled_at_rank"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_enrolled_at_month_student_id), 2).alias("student_gpa_vs_enrolled_at_month_student_id_avg"),
        F.rank().over(w_all_by_student_gpa).alias("student_gpa_rank_2"),
    )
)

annotated = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at_decile",
        "rows_in_enrolled_at_month_student_id",
        "next_student_gpa",
        "distinct_course_id_in_enrolled_at_month_student_",
        "student_gpa_avg_2_prior",
        "enrolled_at_rank",
        "student_gpa_rank_2",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

with_trends_2 = (
    annotated
    .select(
        "*",
        F.round(F.avg("next_student_gpa").over(w_enrolled_at_quarter), 2).alias("enrolled_at_quarter_avg_next_student_gpa"),
        F.lead("student_gpa_avg_2_prior").over(w_enrolled_at_quarter_by_next).alias("next_student_gpa_avg_2_prior"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_quarter)).alias("distinct_course_id_in_enrolled_at_quarter"),
        F.dense_rank().over(w_enrolled_at_quarter_by_next).alias("next_student_gpa_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_distinct_course_id_in), 3).alias("distinct_course_id_in_enrolled_at_month_student__2"),
        F.max("student_gpa_avg_2_prior").over(w_enrolled_at_quarter_by_next_2).alias("max_student_gpa_avg_2_prior_so_far"),
        F.min("enrolled_at_quarter").over(w_enrolled_at_rank_course_id).alias("enrolled_at_rank_course_id_min_enrolled_at_quart"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at_decile",
        "rows_in_enrolled_at_month_student_id",
        "next_student_gpa",
        "enrolled_at_rank",
        "enrolled_at_quarter_avg_next_student_gpa",
        "next_student_gpa_avg_2_prior",
        "distinct_course_id_in_enrolled_at_quarter",
        "next_student_gpa_dense_rank",
        "distinct_course_id_in_enrolled_at_month_student__2",
        "max_student_gpa_avg_2_prior_so_far",
        "enrolled_at_rank_course_id_min_enrolled_at_quart",
        (F.col("enrolled_at_quarter") - F.col("student_gpa_rank_2")).alias("enrolled_at_quarter_minus_student_gpa_rank_2"),
    )
)
