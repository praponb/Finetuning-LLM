from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id_term_by_session_s = Window.partitionBy("session_id", "term").orderBy("session_start", "term", "session_id")
w_session_id_term_by_session_s_2 = Window.partitionBy("session_id", "term").orderBy("session_start", "term", "session_id").rowsBetween(-2, Window.currentRow)

prepared = (
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

with_prev = prepared.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_term_running))

session_summary = (
    sessionized
    .groupBy("term", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    session_summary
    .select(
        "*",
        F.ntile(5).over(w_session_id_term_by_session_s).alias("session_start_quintile"),
        F.ntile(10).over(w_session_id_term_by_session_s).alias("session_start_decile"),
        F.sum("session_id").over(w_session_id_term_by_session_s_2).alias("session_id_sum_3"),
        F.row_number().over(w_session_id_term_by_session_s).alias("session_start_seq"),
        F.round(F.percent_rank().over(w_session_id_term_by_session_s), 3).alias("session_start_percent_rank"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["session_start_quintile", "session_start_seq"], ["session_start_quintile"], ["session_start_seq"]],
        "session_start_quintile", "session_start_seq",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("term") == "2025T1", 1)) / F.count("*"), 1).alias("pct_term_2025t1"),
        F.count("*").alias("n_rows"),
        F.countDistinct("term").alias("distinct_terms"),
    )
    .orderBy(
        F.col("session_start_quintile").asc_nulls_last(),
        F.col("session_start_seq").asc_nulls_last(),
    )
)
