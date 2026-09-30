from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_gpa_distance = Window.orderBy("gpa_distance", "id")
w_is_international_by_enrolled = Window.partitionBy("is_international").orderBy("enrolled_on")

base_students = (
    df_students
    .filter(F.col("is_international"))
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "is_international",
        F.lower(F.regexp_replace(F.trim(F.col("student_name")), " ", "_")).alias("student_name_slug"),
        F.abs(F.col("gpa") - 2.9).alias("gpa_distance"),
        F.abs(F.col("gpa") - 3.2).alias("gpa_distance_2"),
    )
)

ranked = (
    base_students
    .select(
        "id",
        "enrolled_on",
        "student_name_slug",
        "gpa_distance",
        F.lag("gpa_distance_2").over(w_all_by_gpa_distance).alias("prev_gpa_distance_2"),
        F.round(F.cume_dist().over(w_is_international_by_enrolled), 3).alias("enrolled_on_cume_dist"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(~F.col("term").isin("2024T1", "2024T2"), 1)).alias("enrollment_term_count"),
    )
)

with_stats = (
    ranked.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

with_children = (
    with_stats.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.student_name_slug",
        "s.prev_gpa_distance_2",
        "s.enrolled_on_cume_dist",
        "s.enrollment_term_count",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.score").alias("enrollment_score"),
    )
)

enrollment_course_id_stats = (
    with_children
    .groupBy("enrollment_course_id")
    .agg(F.percentile_approx("enrolled_on_cume_dist", 0.5).alias("median_enrolled_on_cume_dist"))
)

above_average = (
    with_children.alias("s")
    .join(enrollment_course_id_stats.alias("g"), F.col("g.enrollment_course_id") == F.col("s.enrollment_course_id"), "inner")
    .filter(F.col("s.enrolled_on_cume_dist") >= F.col("g.median_enrolled_on_cume_dist"))
    .select(
        "s.*",
        "g.median_enrolled_on_cume_dist",
        F.round(F.col("s.enrolled_on_cume_dist") - F.col("g.median_enrolled_on_cume_dist"), 2).alias("enrolled_on_cume_dist_minus_median"),
    )
)

result = (
    above_average
    .filter(F.col("enrollment_score") > 16.3)
    .select(
        "id",
        "prev_gpa_distance_2",
        "enrolled_on_cume_dist",
        "enrollment_term_count",
        "enrollment_id",
        "enrollment_course_id",
        "enrollment_score",
        "median_enrolled_on_cume_dist",
        "enrolled_on_cume_dist_minus_median",
        F.round(F.col("enrollment_score") / F.nullif(F.col("enrollment_term_count"), F.lit(0)), 3).alias("enrollment_score_per_enrollment_term_count"),
    )
    .orderBy("enrollment_score_per_enrollment_term_count")
)
