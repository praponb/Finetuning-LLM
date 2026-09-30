from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_enrollments
    .filter(F.col("grade_status") == "INCOMPLETE")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
    )
)

with_prev = recent.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_course_id_running))

result = (
    sessionized
    .groupingSets(
        [["session_id", "course_id", "new_session_flag"], ["session_id"], []],
        "session_id", "course_id", "new_session_flag",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("term") == "2024T1", 1)) / F.count("*"), 1).alias("pct_term_2024t1"),
        F.sum("new_session_flag").alias("total_new_session_flag"),
    )
    .orderBy(
        F.col("session_id").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("new_session_flag").asc_nulls_last(),
    )
)
