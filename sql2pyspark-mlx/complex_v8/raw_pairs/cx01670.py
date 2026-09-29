from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_enrolled_at_month_course_id = Window.partitionBy(F.date_trunc("month", "enrolled_at"), "course_id")
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
)

windowed = (
    scoped_enrollments
    .select(
        "*",
        F.lag("enrolled_at").over(w_all_by_enrolled_at).alias("prev_enrolled_at"),
        F.round(F.avg("score").over(w_enrolled_at_month_course_id), 2).alias("enrolled_at_month_course_id_avg_score"),
        F.rank().over(w_grade_status_by_score).alias("score_rank"),
        F.row_number().over(w_all_by_enrolled_at).alias("enrolled_at_seq"),
    )
)

joined = (
    windowed.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", F.col("c.level").alias("course_level"))
)

result = (
    joined
    .rollup("student_id", "course_level")
    .agg(
        F.grouping("student_id").alias("is_total_student_id"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.max("enrolled_at_seq").alias("max_enrolled_at_seq"),
    )
    .select(
        "student_id",
        F.coalesce(F.col("course_level"), F.lit("ALL")).alias("course_level"),
        "is_total_student_id",
        "n_rows",
        "grade_status_list",
        "max_enrolled_at_seq",
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("course_level").asc_nulls_last(),
    )
)
