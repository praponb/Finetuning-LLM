from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_student_id_grade_status_by_e = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_all_by_enrolled_at_2 = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_student_id_grade_status_by_e_2 = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at").asc_nulls_last())
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc_nulls_first())
w_all_by_enrolled_at_3 = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_all_by_enrolled_at_ties_2 = Window.orderBy(F.col("enrolled_at").asc_nulls_last())

scoped = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

enriched_student = (
    scoped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.cohort") == "C2022")
    .select("s.*", F.col("st.cohort").alias("student_cohort"))
)

with_windows = (
    enriched_student
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "enrolled_at_date",
        F.row_number().over(w_all_by_enrolled_at).alias("seq_no"),
        F.row_number().over(w_student_id_grade_status_by_e).alias("rn"),
        F.dense_rank().over(w_all_by_enrolled_at_2).alias("enrolled_at_dense_rank"),
        F.rank().over(w_all_by_enrolled_at).alias("enrolled_at_rank"),
        F.rank().over(w_student_id_grade_status_by_e_2).alias("enrolled_at_rank_2"),
        F.dense_rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_dense_rank_2"),
        F.lead("enrolled_at", 3).over(w_student_id_grade_status_by_e).alias("next3_enrolled_at"),
        F.lead("enrolled_at", 2).over(w_all_by_enrolled_at_3).alias("next2_enrolled_at"),
        F.rank().over(w_all_by_enrolled_at_ties_2).alias("enrolled_at_rank_3"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "enrolled_at_date",
        "rn",
        "enrolled_at_rank",
        "enrolled_at_rank_2",
        "enrolled_at_dense_rank_2",
        "next3_enrolled_at",
        "next2_enrolled_at",
    )
    .orderBy(F.col("rn").desc())
)
