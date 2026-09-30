from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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
    )
)

with_prev = filtered_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_term_running))

labeled = (
    sessionized
    .filter(F.col("session_id").between(2, 4))
    .select(
        "*",
        F.year(F.col("prev_enrolled_at")).alias("prev_enrolled_at_year"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
)

result = (
    labeled
    .groupingSets(
        [["term", "session_id", "student_id"], ["term"], ["session_id"], []],
        "term", "session_id", "student_id",
    )
    .agg(
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.round(F.stddev("new_session_flag"), 2).alias("stddev_new_session_flag"),
        F.count(F.when(F.col("grade_status") == "INCOMPLETE", 1)).alias("n_grade_status_incomplete"),
    )
    .orderBy(
        F.col("term").asc_nulls_last(),
        F.col("session_id").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
