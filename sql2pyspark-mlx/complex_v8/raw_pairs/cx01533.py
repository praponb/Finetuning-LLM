from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id", "enrollment_id")
w_enrollment_id = Window.partitionBy("enrollment_id")

prepared = (
    df_enrollments
    .filter(F.col("student_id") <= 18)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

exploded_detail_enrollments = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

with_windows = (
    exploded_detail_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "enrollment_id",
        "enrollment_term",
        F.row_number().over(w_all_by_enrolled_at).alias("seq_no"),
        F.sum("enrollment_score").over(w_enrollment_id).alias("enrollment_id_sum_enrollment_score"),
    )
)

joined_course = (
    with_windows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(~F.col("c.level").isin("advanced", "core"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrollment_id",
        "s.enrollment_term",
        "s.seq_no",
        F.col("c.level").alias("course_level"),
    )
)

result = (
    joined_course
    .groupingSets(
        [["course_id", "enrollment_term"], ["course_id"], ["enrollment_term"], []],
        "course_id", "enrollment_term",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("enrollment_term")).alias("enrollment_term_list"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.max("seq_no").alias("max_seq_no"),
    )
    .select(
        "course_id",
        F.coalesce(F.col("enrollment_term"), F.lit("ALL")).alias("enrollment_term"),
        "grouping_level",
        "enrollment_term_list",
        "avg_seq_no",
        "max_seq_no",
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("enrollment_term").asc_nulls_last(),
    )
)
