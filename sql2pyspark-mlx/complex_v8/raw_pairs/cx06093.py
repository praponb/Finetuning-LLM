from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_year = Window.partitionBy(F.year("enrolled_at"))
w_student_id_grade_status_by_e = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at").desc())
w_course_id_grade_status_by_sc = Window.partitionBy("course_id", "grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc())
w_student_id_grade_status_by_e_2 = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at").desc(), "id")
w_student_id = Window.partitionBy("student_id")
w_course_id_grade_status_by_sc_2 = Window.partitionBy("course_id", "grade_status").orderBy(F.col("score").desc(), "id")

valid_enrollments = (
    df_enrollments
    .filter((F.col("score") < 85.2) & (F.col("course_id").isNotNull()))
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

windowed = (
    valid_enrollments
    .select(
        "*",
        F.round(F.col("score") - F.avg("score").over(w_enrolled_at_year), 2).alias("score_vs_enrolled_at_year_avg"),
        F.dense_rank().over(w_student_id_grade_status_by_e).alias("enrolled_at_dense_rank"),
        F.first("enrolled_at", ignorenulls=True).over(w_course_id_grade_status_by_sc).alias("first_known_enrolled_at"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
        F.ntile(5).over(w_student_id_grade_status_by_e_2).alias("enrolled_at_quintile"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_year), F.lit(0)), 4).alias("score_share_of_enrolled_at_year"),
        F.count(F.lit(1)).over(w_enrolled_at_year).alias("rows_in_enrolled_at_year"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.rank().over(w_student_id_grade_status_by_e_2).alias("enrolled_at_rank"),
        F.lead("score", 2, 0).over(w_course_id_grade_status_by_sc_2).alias("next2_score"),
        F.rank().over(w_student_id_grade_status_by_e).alias("enrolled_at_rank_2"),
        F.rank().over(w_course_id_grade_status_by_sc_2).alias("score_rank"),
    )
)

top_rows = (
    windowed
    .filter(F.col("score_rank") <= 2)
    .drop("score_rank")
)

result = (
    top_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "score_vs_enrolled_at_year_avg",
        "enrolled_at_dense_rank",
        "first_known_enrolled_at",
        "enrolled_at_cume_dist",
        "enrolled_at_quintile",
        "score_vs_student_id_avg",
        "score_share_of_enrolled_at_year",
        "rows_in_enrolled_at_year",
        "score_share_of_student_id",
        "enrolled_at_rank",
        "next2_score",
        F.date_trunc("quarter", F.col("first_known_enrolled_at")).alias("first_known_enrolled_at_quarter"),
    )
    .orderBy(
        F.col("rows_in_enrolled_at_year").desc(),
        "score_vs_enrolled_at_year_avg",
    )
)
