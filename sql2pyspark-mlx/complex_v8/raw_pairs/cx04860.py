from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_enrolled_at_ru = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status = Window.partitionBy("grade_status")
w_all_by_score_running = Window.orderBy(F.col("score").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_running_count_running = Window.orderBy(F.col("running_count").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_grade_status_by_r = Window.partitionBy("student_id", "grade_status").orderBy("running_count", "id")

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

windowed = (
    cleaned_enrollments
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.count(F.lit(1)).over(w_student_id_by_enrolled_at_ru).alias("running_count"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.last("score", ignorenulls=True).over(w_all_by_score_running).alias("last_known_score"),
    )
)

with_trends = (
    windowed
    .withColumn("max_last_known_score_so_far", F.max("last_known_score").over(w_all_by_running_count_running))
    .withColumn("running_count_dense_rank", F.dense_rank().over(w_student_id_grade_status_by_r))
)

top_per_group = (
    with_trends
    .filter((F.col("running_count_dense_rank") <= 2) & (F.col("max_last_known_score_so_far") >= 200.0))
    .drop("running_count_dense_rank")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
        F.count("*").alias("enrollment_count"),
    )
)

with_counts = (
    top_per_group.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.first_enrollment_enrolled_at_2",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "rows_in_grade_status",
        "last_known_score",
        "max_last_known_score_so_far",
        "first_enrollment_enrolled_at_2",
        F.round(F.col("last_known_score") * 0.9, 2).alias("last_known_score_adj"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
    )
    .orderBy(F.col("first_enrollment_enrolled_at_2").desc())
)
