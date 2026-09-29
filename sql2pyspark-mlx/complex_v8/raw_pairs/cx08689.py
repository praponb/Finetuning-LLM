from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_course_id = Window.partitionBy("student_id", "course_id")

prepared_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("INCOMPLETE", "PASS", "WITHDRAWN")) & (F.col("score") >= 38.1))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.concat_ws(" / ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
    )
)

with_windows = (
    prepared_enrollments
    .select(
        "*",
        F.lag("enrolled_at").over(w_student_id_by_enrolled_at).alias("prev_enrolled_at"),
        F.ntile(10).over(w_student_id_by_enrolled_at).alias("enrolled_at_decile"),
        F.size(F.collect_set("grade_status").over(w_student_id_course_id)).alias("distinct_grade_status_in_student_id_course_id"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at_2"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    with_windows.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        "e.last_enrollment_enrolled_at_2",
        "e.first_enrollment_enrolled_at",
    )
)

result = (
    summarized_rows
    .rollup("enrolled_at_decile", "student_id", "grade_status")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        "enrolled_at_decile",
        "student_id",
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "grouping_level",
        "distinct_ids",
    )
    .orderBy(
        F.col("enrolled_at_decile").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
