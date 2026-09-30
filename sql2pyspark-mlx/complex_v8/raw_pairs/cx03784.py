from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("term").orderBy("avg_enrollment_score", "id")
w_student_id_enrollment_count = Window.partitionBy("student_id", "enrollment_count").orderBy(F.col("enrollment_count").desc())
w_student_id = Window.partitionBy("student_id")
w_student_id_enrollment_count_2 = Window.partitionBy("student_id", "enrollment_count").orderBy(F.col("enrollment_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_enrollments
    .filter((~F.col("term").isin("2024T2", "2025T2")) & (F.col("grade_status") != "PASS"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 39.4).alias("score_distance"),
    )
)

with_activity = (
    base.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.length(F.trim(F.col("st.student_name"))) >= 8) & (F.col("st.cohort") == "C2024"), "left_semi")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.count("*").alias("enrollment_count_2"),
    )
)

with_counts = (
    with_activity.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.avg_enrollment_score",
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

unique_rows = (
    with_counts
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    unique_rows
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "enrollment_count",
        "avg_enrollment_score",
        "enrollment_count_2",
        F.round(F.percent_rank().over(w_student_id_enrollment_count), 3).alias("enrollment_count_percent_rank"),
        F.round(F.cume_dist().over(w_student_id_enrollment_count), 3).alias("enrollment_count_cume_dist"),
        F.round(F.avg("enrollment_count_2").over(w_student_id), 2).alias("student_id_avg_enrollment_count_2"),
        F.max("enrollment_count").over(w_student_id_enrollment_count_2).alias("max_enrollment_count_so_far"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
    )
)

result = (
    metrics
    .filter(F.col("max_enrollment_count_so_far") < 72)
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "enrollment_count",
        "avg_enrollment_score",
        "enrollment_count_2",
        "enrollment_count_percent_rank",
        "enrollment_count_cume_dist",
        "student_id_avg_enrollment_count_2",
        "max_enrollment_count_so_far",
    )
    .orderBy(F.col("student_id_avg_enrollment_count_2").desc())
)
