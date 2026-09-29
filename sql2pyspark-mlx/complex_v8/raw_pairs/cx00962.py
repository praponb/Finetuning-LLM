from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at_quarter").asc_nulls_last(), "id")
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_student_id_course_id_by_enro = Window.partitionBy("student_id", "course_id").orderBy(F.col("enrolled_at_quarter").desc(), "id").rowsBetween(-6, Window.currentRow)
w_enrolled_at_month_by_enrolle = Window.partitionBy(F.date_trunc("month", "enrolled_at")).orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_grade_status = Window.partitionBy("course_id", "grade_status")
w_course_id_enrolled_at_quarte = Window.partitionBy("course_id", "enrolled_at_quarter").orderBy(F.col("enrolled_at_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_grade_status = Window.partitionBy("term", "grade_status")
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at_quarter").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_first_enrolled_at_quarter = Window.partitionBy("first_enrolled_at_quarter")
w_term_student_id_by_distinct = Window.partitionBy("term", "student_id").orderBy(F.col("distinct_student_id_in_course_id_grade_status").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prev3_enrolled_at_year_by_ro = Window.partitionBy(F.year("prev3_enrolled_at")).orderBy("rows_in_term_grade_status", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_enrollments = (
    df_enrollments
    .filter((F.col("course_id") >= 2) & (F.col("score") <= 78.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
    )
)

with_trends = (
    filtered_enrollments
    .select(
        "*",
        F.row_number().over(w_grade_status_by_enrolled_at).alias("rn"),
        F.round(F.cume_dist().over(w_student_id_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
        F.sum("score").over(w_student_id_course_id_by_enro).alias("score_sum_7"),
        F.lag("enrolled_at", 3).over(w_enrolled_at_month_by_enrolle).alias("prev3_enrolled_at"),
        F.size(F.collect_set("student_id").over(w_course_id_grade_status)).alias("distinct_student_id_in_course_id_grade_status"),
        F.first("enrolled_at_quarter").over(w_course_id_enrolled_at_quarte).alias("first_enrolled_at_quarter"),
        F.count(F.lit(1)).over(w_term_grade_status).alias("rows_in_term_grade_status"),
        F.round(F.avg("score").over(w_grade_status_by_enrolled_at_2), 2).alias("score_avg_7"),
    )
)

windowed = (
    with_trends
    .select(
        "*",
        F.round(F.col("enrolled_at_quarter") - F.avg("enrolled_at_quarter").over(w_first_enrolled_at_quarter), 2).alias("enrolled_at_quarter_vs_first_enrolled_at_quarter"),
        F.round(F.col("enrolled_at_cume_dist") - F.avg("enrolled_at_cume_dist").over(w_first_enrolled_at_quarter), 2).alias("enrolled_at_cume_dist_vs_first_enrolled_at_quart"),
        F.min("score").over(w_term_student_id_by_distinct).alias("min_score_so_far"),
        F.size(F.collect_set("term").over(w_first_enrolled_at_quarter)).alias("distinct_term_in_first_enrolled_at_quarter"),
        F.nth_value("rows_in_term_grade_status", 2).over(w_prev3_enrolled_at_year_by_ro).alias("second_rows_in_term_grade_status"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_quarter",
        "grade_status_ref",
        "rn",
        "enrolled_at_cume_dist",
        "prev3_enrolled_at",
        "distinct_student_id_in_course_id_grade_status",
        "first_enrolled_at_quarter",
        "rows_in_term_grade_status",
        "enrolled_at_quarter_vs_first_enrolled_at_quarter",
        "enrolled_at_cume_dist_vs_first_enrolled_at_quart",
        "min_score_so_far",
        "distinct_term_in_first_enrolled_at_quarter",
        "second_rows_in_term_grade_status",
        F.round((F.unix_timestamp(F.col("enrolled_at")) - F.unix_timestamp(F.col("prev3_enrolled_at"))) / 60, 1).alias("minutes_prev3_enrolled_at_to_enrolled_at"),
    )
    .orderBy("grade_status_ref")
)
