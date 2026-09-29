from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_course_id_by_en = Window.partitionBy("grade_status", "course_id").orderBy("enrolled_at_date", "id")
w_course_id_by_score = Window.partitionBy("course_id").orderBy("score", "id")
w_enrolled_at_date_year_studen = Window.partitionBy(F.year("enrolled_at_date"), "student_id")
w_student_id_course_id_by_enro = Window.partitionBy("student_id", "course_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_course_id_by_en_2 = Window.partitionBy("grade_status", "course_id").orderBy("enrolled_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (~F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

without_matches = (
    base.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (~F.col("e.term").isin("2025T2", "2024T2")) & (F.col("e.score") > 8.8), "left_anti")
    .filter(F.col("score") <= 78.8)
)

screened = (
    without_matches.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status") == "INCOMPLETE"), "left_semi")
)

metrics = (
    screened
    .select(
        "id",
        "score",
        "grade_status",
        F.row_number().over(w_grade_status_course_id_by_en).alias("rn"),
        F.lag("enrolled_at_date", 2).over(w_course_id_by_score).alias("prev2_enrolled_at_date"),
        F.max("score").over(w_enrolled_at_date_year_studen).alias("enrolled_at_date_year_student_id_max_score"),
        F.count(F.lit(1)).over(w_enrolled_at_date_year_studen).alias("rows_in_enrolled_at_date_year_student_id"),
        F.last("enrolled_at").over(w_student_id_course_id_by_enro).alias("last_enrolled_at"),
        F.nth_value("score", 2).over(w_grade_status_course_id_by_en_2).alias("second_score"),
    )
)

result = (
    metrics
    .rollup("grade_status", "rn")
    .agg(
        F.round(F.stddev("rows_in_enrolled_at_date_year_student_id"), 2).alias("stddev_rows_in_enrolled_at_date_year_student_id"),
        F.max("last_enrolled_at").alias("last_last_enrolled_at"),
    )
    .select(
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "rn",
        "stddev_rows_in_enrolled_at_date_year_student_id",
        "last_last_enrolled_at",
    )
    .orderBy(F.col("grade_status").asc_nulls_last(), F.col("rn").asc_nulls_last())
)
