from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_score_full = Window.partitionBy("term").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_score = Window.partitionBy("term").orderBy("score", "id")
w_course_id = Window.partitionBy("course_id")
w_term_by_score_moving_6 = Window.partitionBy("term").orderBy("score", "id").rowsBetween(-6, Window.currentRow)
w_latest = Window.partitionBy("term", "student_major").orderBy("score_filled_vs_course_id_avg", "id")

filtered = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.last("score").over(w_term_by_score_full).alias("last_score"),
        F.round(F.percent_rank().over(w_term_by_score), 3).alias("score_percent_rank"),
        F.ntile(3).over(w_term_by_score).alias("score_tercile"),
        F.round(F.col("score_filled") - F.avg("score_filled").over(w_course_id), 2).alias("score_filled_vs_course_id_avg"),
        F.round(F.avg("score").over(w_term_by_score_moving_6), 2).alias("score_avg_7"),
        F.row_number().over(w_term_by_score).alias("score_seq"),
    )
)

combined_details_student = (
    sequenced.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.score_filled",
        "s.term_prefix",
        "s.last_score",
        "s.score_tercile",
        "s.score_filled_vs_course_id_avg",
        "s.score_avg_7",
        F.col("st.major").alias("student_major"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

deduped = (
    combined_details_student
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

exploded_detail = (
    deduped.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score_filled",
        "s.term_prefix",
        "s.score_filled_vs_course_id_avg",
        "s.student_is_international",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

result = (
    exploded_detail
    .filter(F.col("enrollment_term").isin("2025T2", "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score_filled",
        "term_prefix",
        "score_filled_vs_course_id_avg",
        "student_is_international",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_score",
        "enrollment_term",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
    .orderBy(F.col("enrollment_id").asc_nulls_last())
)
