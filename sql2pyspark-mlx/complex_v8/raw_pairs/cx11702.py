from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_score_prior = Window.partitionBy("grade_status").orderBy("score", "id").rowsBetween(-6, -1)
w_grade_status = Window.partitionBy("grade_status")
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")
w_student_id_course_id_by_scor_2 = Window.partitionBy("student_id", "course_id").orderBy(F.col("score").desc(), "id").rowsBetween(-6, Window.currentRow)

recent_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-18 08:00:00").cast("timestamp"))
)

ranked = (
    recent_enrollments
    .select(
        "*",
        F.last("enrolled_at", ignorenulls=True).over(w_student_id_course_id_by_scor).alias("last_known_enrolled_at"),
        F.sum("score").over(w_grade_status_by_score_prior).alias("score_sum_6_prior"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.row_number().over(w_grade_status_by_score).alias("score_seq"),
        F.round(F.avg("score").over(w_student_id_course_id_by_scor_2), 2).alias("score_avg_7"),
        F.round(F.avg("score").over(w_grade_status), 2).alias("grade_status_avg_score"),
    )
)

result = (
    ranked
    .groupBy("course_id", "last_known_enrolled_at")
    .pivot("grade_status", ["FAIL", "WITHDRAWN", "PASS", "INCOMPLETE"])
    .agg(F.avg("score_sum_6_prior").alias("avg_v"), F.count("score_sum_6_prior").alias("n"))
)
