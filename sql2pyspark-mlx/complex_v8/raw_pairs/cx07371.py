from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_or_unknown_course_id_by = Window.partitionBy("term_or_unknown", "course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_term_or_unknown_by = Window.partitionBy("course_id", "term_or_unknown").orderBy(F.col("enrolled_at").desc())
w_term_or_unknown_course_id_by_2 = Window.partitionBy("term_or_unknown", "course_id").orderBy("enrolled_at")
w_term_or_unknown_student_id_b = Window.partitionBy("term_or_unknown", "student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_term_or_unknown_course_id_by_3 = Window.partitionBy("term_or_unknown", "course_id").orderBy("enrolled_at", "id")
w_term_or_unknown_student_id_b_2 = Window.partitionBy("term_or_unknown", "student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_term_or_unknown_by_2 = Window.partitionBy("course_id", "term_or_unknown").orderBy(F.col("enrolled_at").desc(), "id")

valid = (
    df_enrollments
    .filter(F.col("score") < 59.8)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.nth_value("enrolled_at", 2).over(w_term_or_unknown_course_id_by).alias("second_enrolled_at"),
        F.dense_rank().over(w_course_id_term_or_unknown_by).alias("enrolled_at_dense_rank"),
        F.rank().over(w_term_or_unknown_course_id_by_2).alias("enrolled_at_rank"),
        F.row_number().over(w_term_or_unknown_student_id_b).alias("enrolled_at_seq"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_term_or_unknown_course_id_by_3))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.first("enrolled_at", ignorenulls=True).over(w_term_or_unknown_student_id_b_2).alias("first_known_enrolled_at"),
        F.round(F.percent_rank().over(w_term_or_unknown_course_id_by_3), 3).alias("enrolled_at_percent_rank"),
        F.last("enrolled_at").over(w_term_or_unknown_course_id_by).alias("last_enrolled_at"),
        F.round(F.cume_dist().over(w_course_id_term_or_unknown_by_2), 3).alias("enrolled_at_cume_dist"),
        F.lead("enrolled_at").over(w_course_id_term_or_unknown_by_2).alias("next_enrolled_at"),
        F.row_number().over(w_term_or_unknown_course_id_by_3).alias("seq_no"),
        F.dense_rank().over(w_term_or_unknown_course_id_by_2).alias("enrolled_at_dense_rank_4"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "second_enrolled_at",
        "enrolled_at_dense_rank",
        "enrolled_at_rank",
        "enrolled_at_seq",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_percent_rank",
        "last_enrolled_at",
        "enrolled_at_cume_dist",
        "next_enrolled_at",
        "enrolled_at_dense_rank_4",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.round((F.unix_timestamp(F.col("next_enrolled_at")) - F.unix_timestamp(F.col("last_enrolled_at"))) / 3600, 1).alias("hours_last_enrolled_at_to_next_enrolled_at"),
    )
)
