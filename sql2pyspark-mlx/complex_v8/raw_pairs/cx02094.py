from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_grade_status_group = Window.partitionBy("grade_status_group")
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_student_gpa_ties = Window.orderBy(F.col("student_gpa").desc())
w_student_id_by_enrolled_at_ti = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc())
w_course_id_student_id_by_stud = Window.partitionBy("course_id", "student_id").orderBy("student_gpa", "id")
w_course_id_student_id_by_stud_2 = Window.partitionBy("course_id", "student_id").orderBy("student_gpa", "id").rowsBetween(-1, Window.currentRow)
w_all_by_student_gpa = Window.orderBy(F.col("student_gpa").desc(), "id")
w_student_gpa_rank_course_id_b = Window.partitionBy("student_gpa_rank", "course_id").orderBy(F.col("prev_student_gpa").desc(), "id")
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc_nulls_first())
w_student_gpa_dense_rank_grade = Window.partitionBy("student_gpa_dense_rank", "grade_status_group")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id")

base = (
    df_enrollments
    .filter(F.col("term").isin("2024T2", "2025T1", "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.when(F.col("grade_status").isin("INCOMPLETE", "PASS"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

combined_details_student = (
    base.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.enrolled_at_label",
        "s.grade_status_group",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

metrics = (
    combined_details_student
    .select(
        "*",
        F.lead("student_gpa", 2, 0).over(w_course_id_by_enrolled_at).alias("next2_student_gpa"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_grade_status_group), 2).alias("student_gpa_vs_grade_status_group_avg"),
        F.lag("student_gpa", 3).over(w_student_id_by_enrolled_at).alias("prev3_student_gpa"),
        F.rank().over(w_all_by_student_gpa_ties).alias("student_gpa_rank"),
        F.round(F.percent_rank().over(w_student_id_by_enrolled_at_ti), 3).alias("enrolled_at_percent_rank"),
        F.lag("student_gpa").over(w_course_id_student_id_by_stud).alias("prev_student_gpa"),
        F.max("student_gpa").over(w_course_id_student_id_by_stud_2).alias("student_gpa_max_2"),
        F.dense_rank().over(w_all_by_student_gpa).alias("student_gpa_dense_rank"),
    )
)

windowed = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "grade_status_group",
        "student_gpa",
        "next2_student_gpa",
        "student_gpa_vs_grade_status_group_avg",
        "prev3_student_gpa",
        "student_gpa_rank",
        "enrolled_at_percent_rank",
        "prev_student_gpa",
        "student_gpa_max_2",
        "student_gpa_dense_rank",
        F.rank().over(w_student_gpa_rank_course_id_b).alias("prev_student_gpa_rank"),
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank"),
        F.count(F.lit(1)).over(w_student_gpa_dense_rank_grade).alias("rows_in_student_gpa_dense_rank_grade_status_grou"),
        F.rank().over(w_all_by_enrolled_at).alias("enrolled_at_rank_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "grade_status_group",
        "student_gpa",
        "next2_student_gpa",
        "prev3_student_gpa",
        "student_gpa_rank",
        "prev_student_gpa",
        "student_gpa_max_2",
        "student_gpa_dense_rank",
        "prev_student_gpa_rank",
        "enrolled_at_rank",
        "enrolled_at_rank_2",
        F.round(F.col("enrolled_at_percent_rank") * 1.1, 2).alias("enrolled_at_percent_rank_adj"),
    )
    .orderBy(F.col("id").desc_nulls_first())
    .limit(20)
)
