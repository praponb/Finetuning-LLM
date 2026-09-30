from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status")
w_term_student_id = Window.partitionBy("term", "student_id")
w_all_by_days_since_enrolled_a = Window.orderBy("days_since_enrolled_at", "id")
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_year_by_enrolled = Window.partitionBy(F.year("enrolled_at")).orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-1, Window.currentRow)

prepared = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

sequenced = (
    prepared
    .select(
        "id",
        "student_id",
        "course_id",
        F.round(F.avg("days_since_enrolled_at").over(w_grade_status), 2).alias("grade_status_avg_days_since_enrolled_at"),
        F.round(F.col("days_since_enrolled_at") / F.nullif(F.sum("days_since_enrolled_at").over(w_term_student_id), F.lit(0)), 4).alias("days_since_enrolled_at_share_of_term_student_id"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_days_since_enrolled_a))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.avg("days_since_enrolled_at").over(w_term_student_id), 2).alias("term_student_id_avg_days_since_enrolled_at"),
        F.round(F.col("days_since_enrolled_at") - F.avg("days_since_enrolled_at").over(w_grade_status), 2).alias("days_since_enrolled_at_vs_grade_status_avg"),
        F.sum("days_since_enrolled_at").over(w_grade_status_by_enrolled_at).alias("running_days_since_enrolled_at"),
        F.min("days_since_enrolled_at").over(w_grade_status).alias("grade_status_min_days_since_enrolled_at"),
        F.round(F.avg("days_since_enrolled_at").over(w_enrolled_at_year_by_enrolled), 2).alias("days_since_enrolled_at_avg_2"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "student_id",
        "course_id",
        "days_since_enrolled_at_share_of_term_student_id",
        "minutes_since_prev_enrolled_at",
        "term_student_id_avg_days_since_enrolled_at",
        "grade_status_min_days_since_enrolled_at",
        "days_since_enrolled_at_avg_2",
    )
    .orderBy(F.col("term_student_id_avg_days_since_enrolled_at").desc(), "id")
    .limit(10)
)
