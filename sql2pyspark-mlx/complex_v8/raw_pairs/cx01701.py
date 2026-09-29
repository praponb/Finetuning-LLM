from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_course_credits_prior = Window.orderBy("course_credits", "id").rowsBetween(-2, -1)
w_course_credits = Window.partitionBy("course_credits")
w_student_id_term_by_course_cr = Window.partitionBy("student_id", "term").orderBy(F.col("course_credits").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_course_credits_runnin = Window.orderBy("course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id")
w_student_id_grade_status_by_c = Window.partitionBy("student_id", "grade_status").orderBy(F.col("course_credits").asc_nulls_last())
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-3, -1)
w_grade_status_by_enrolled_at_3 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

combined_details_course = (
    cleaned.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.credits").alias("course_credits"),
    )
)

windowed = (
    combined_details_course
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        F.round(F.avg("course_credits").over(w_all_by_course_credits_prior), 2).alias("course_credits_avg_2_prior"),
        F.size(F.collect_set("course_id").over(w_course_credits)).alias("distinct_course_id_in_course_credits"),
        F.max("course_credits").over(w_student_id_term_by_course_cr).alias("course_credits_max_4"),
        F.count(F.lit(1)).over(w_course_credits).alias("rows_in_course_credits"),
        F.count(F.lit(1)).over(w_all_by_course_credits_runnin).alias("running_count"),
        F.sum("course_credits").over(w_course_credits).alias("course_credits_sum_course_credits"),
        F.row_number().over(w_grade_status_by_enrolled_at).alias("rn"),
        F.round(F.percent_rank().over(w_student_id_grade_status_by_c), 3).alias("course_credits_percent_rank"),
        F.round(F.avg("course_credits").over(w_grade_status_by_enrolled_at_2), 2).alias("course_credits_avg_3_prior"),
        F.round(F.avg("course_credits").over(w_course_credits), 2).alias("course_credits_avg_course_credits"),
        F.sum("course_credits").over(w_grade_status_by_enrolled_at_3).alias("course_credits_sum_4"),
        F.size(F.collect_set("student_id").over(w_course_credits)).alias("distinct_student_id_in_course_credits"),
        F.size(F.collect_set("grade_status").over(w_course_credits)).alias("distinct_grade_status_in_course_credits"),
    )
)

result = (
    windowed
    .filter(~F.col("term").isin("2025T2", "2025T1"))
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "course_credits_avg_2_prior",
        "course_credits_max_4",
        "rows_in_course_credits",
        "running_count",
        "rn",
        "course_credits_avg_3_prior",
        "course_credits_avg_course_credits",
        "course_credits_sum_4",
        "distinct_student_id_in_course_credits",
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)
