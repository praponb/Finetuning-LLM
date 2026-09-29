from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_year = Window.partitionBy(F.year("enrolled_at"))
w_session_no_by_score = Window.partitionBy("session_no").orderBy("score", "id")

prepared = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

with_prev = prepared.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_course_id_running))

with_trends = (
    sessionized
    .select(
        "*",
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_year), F.lit(0)), 4).alias("score_share_of_enrolled_at_year"),
        F.dense_rank().over(w_session_no_by_score).alias("score_dense_rank"),
    )
)

without_matches = (
    with_trends.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") <= 79.2) & (F.col("e.term").isNotNull()), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "student_id",
        "term",
        "term_len",
        "prev_enrolled_at",
        "starts_session",
        "session_no",
        "score_share_of_enrolled_at_year",
        "score_dense_rank",
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
    )
    .orderBy(F.col("term").asc())
)
