from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_course_id_term_by_score = Window.partitionBy("course_id", "term").orderBy(F.col("score").desc(), "id")
w_student_id_term = Window.partitionBy("student_id", "term")
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_course_id_student_id_by_enro = Window.partitionBy("course_id", "student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_enrolled_at_ru = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_enrolled_at_ti = Window.partitionBy("student_id").orderBy("enrolled_at")
w_course_id_term_by_score_runn = Window.partitionBy("course_id", "term").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_enrollments = (
    df_enrollments
    .filter((F.col("score").between(8.8, 73.0)) & (F.col("student_id") <= 7))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

ranked = (
    prepared_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.round(F.avg("score").over(w_student_id), 2).alias("student_id_avg_score"),
        F.row_number().over(w_course_id_term_by_score).alias("rn"),
        F.count(F.lit(1)).over(w_student_id_term).alias("rows_in_student_id_term"),
        F.lead("score", 2).over(w_student_id_by_enrolled_at).alias("next2_score"),
        F.first("enrolled_at", ignorenulls=True).over(w_course_id_student_id_by_enro).alias("first_known_enrolled_at"),
        F.first("enrolled_at", ignorenulls=True).over(w_student_id_by_enrolled_at_ru).alias("first_known_enrolled_at_2"),
        F.rank().over(w_student_id_by_enrolled_at_ti).alias("enrolled_at_rank"),
        F.last("score", ignorenulls=True).over(w_course_id_term_by_score_runn).alias("last_known_score"),
        F.round(F.percent_rank().over(w_student_id_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
        F.last("enrolled_at", ignorenulls=True).over(w_student_id_by_enrolled_at_ru).alias("last_known_enrolled_at"),
        F.lead("enrolled_at").over(w_student_id_by_enrolled_at).alias("next_enrolled_at"),
    )
)

result = (
    ranked
    .select(
        "id",
        "course_id",
        "score",
        "rn",
        "rows_in_student_id_term",
        "first_known_enrolled_at",
        "enrolled_at_rank",
        "last_known_score",
        "enrolled_at_percent_rank",
        "last_known_enrolled_at",
        "next_enrolled_at",
        F.hour(F.col("next_enrolled_at")).alias("next_enrolled_at_hour"),
    )
    .orderBy(F.col("enrolled_at_rank").asc_nulls_last(), F.col("id").desc())
    .limit(20)
)
