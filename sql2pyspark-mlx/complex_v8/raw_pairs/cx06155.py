from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_terms = Window.partitionBy("distinct_terms")
w_distinct_terms_event_count_b = Window.partitionBy("distinct_terms", "event_count").orderBy(F.col("n_term_in_2024t2_2024t1_2025t1").desc(), "event_count")

recent_enrollments = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(7, 11))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

with_prev = recent_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_term_running))

per_session = (
    sessionized
    .groupBy("term", "session_no")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

per_group = (
    per_session
    .filter(F.col("duration_min") >= 108.0)
    .groupBy("event_count")
    .agg(
        F.sum(F.when(F.col("term").isin("2025T1", "2024T2"), F.col("session_no"))).alias("session_no_term_in_2025t1_2024t2"),
        F.sum(F.when(F.col("term").isin("2024T2", "2024T1", "2025T1"), 1).otherwise(0)).alias("n_term_in_2024t2_2024t1_2025t1"),
        F.round(F.sum("event_count") / F.nullif(F.sum("session_no"), F.lit(0)), 3).alias("event_count_to_session_no_ratio"),
        F.countDistinct("term").alias("distinct_terms"),
    )
)

windowed = (
    per_group
    .select(
        "event_count",
        "n_term_in_2024t2_2024t1_2025t1",
        "event_count_to_session_no_ratio",
        F.sum("distinct_terms").over(w_distinct_terms).alias("distinct_terms_sum_distinct_terms"),
        F.round(F.col("event_count") / F.nullif(F.sum("event_count").over(w_distinct_terms), F.lit(0)), 4).alias("event_count_share_of_distinct_terms"),
        F.row_number().over(w_distinct_terms_event_count_b).alias("rn"),
    )
)

result = (
    windowed
    .select(
        "event_count",
        F.col("event_count_share_of_distinct_terms").alias("m_event_count_share_of_distinct_terms"),
        F.col("event_count_to_session_no_ratio").alias("m_event_count_to_session_no_ratio"),
    )
    .unpivot(
        ids=["event_count"],
        values=["m_event_count_share_of_distinct_terms", "m_event_count_to_session_no_ratio"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("event_count", "measure")
)
