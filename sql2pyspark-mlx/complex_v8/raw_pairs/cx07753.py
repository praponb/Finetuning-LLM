from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

prepared_enrollments = df_enrollments.filter(F.col("course_id").isNotNull())

with_prev = prepared_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_course_id_running))

with_activity = (
    sessionized.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
)

deduped = (
    with_activity
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "burst_id",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
    )
    .orderBy(F.col("grade_status").asc(), F.col("term_clean").asc_nulls_last())
)
