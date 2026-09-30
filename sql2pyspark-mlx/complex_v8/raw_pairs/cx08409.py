from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
    )
)

with_prev = filtered_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_student_id_running))

result = (
    sessionized
    .cube("course_id", "burst_id", "is_new_session")
    .agg(
        F.grouping("course_id").alias("is_total_course_id"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("burst_id").asc_nulls_last(),
        F.col("is_new_session").asc_nulls_last(),
    )
)
