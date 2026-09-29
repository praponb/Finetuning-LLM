from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_cohort_by_enrolled_a = Window.partitionBy("student_cohort").orderBy("enrolled_at", "id")
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc())
w_student_cohort_by_enrolled_a_2 = Window.partitionBy("student_cohort").orderBy("enrolled_at")
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_student_cohort_by_enrolled_a_3 = Window.partitionBy("student_cohort").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_2 = Window.orderBy("enrolled_at", "id")
w_student_cohort_by_enrolled_a_4 = Window.partitionBy("student_cohort").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_ties_2 = Window.orderBy("enrolled_at")

recent_enrollments = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-03-08").cast("date")) & (F.col("score").between(16.3, 85.2)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

enriched_student = (
    recent_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(~F.col("st.is_international"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
    )
)

windowed = (
    enriched_student
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "student_cohort",
        "student_is_international",
        F.ntile(4).over(w_student_cohort_by_enrolled_a).alias("enrolled_at_quartile"),
        F.dense_rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
        F.rank().over(w_student_cohort_by_enrolled_a_2).alias("enrolled_at_rank"),
        F.last("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("last_known_enrolled_at"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.lag("enrolled_at", 2).over(w_student_cohort_by_enrolled_a).alias("prev2_enrolled_at"),
        F.first("enrolled_at").over(w_student_cohort_by_enrolled_a_3).alias("first_enrolled_at"),
        F.lag("enrolled_at", 2).over(w_all_by_enrolled_at_2).alias("prev2_enrolled_at_2"),
        F.first("enrolled_at", ignorenulls=True).over(w_student_cohort_by_enrolled_a_4).alias("first_known_enrolled_at"),
        F.row_number().over(w_all_by_enrolled_at).alias("rn"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.lag("enrolled_at", 2).over(w_all_by_enrolled_at).alias("prev2_enrolled_at_3"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties_2), 3).alias("enrolled_at_percent_rank_2"),
        F.round(F.cume_dist().over(w_student_cohort_by_enrolled_a), 3).alias("enrolled_at_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "term",
        "student_is_international",
        "enrolled_at_dense_rank",
        "enrolled_at_rank",
        "last_known_enrolled_at",
        "minutes_since_prev_enrolled_at",
        "first_enrolled_at",
        "prev2_enrolled_at_2",
        "rn",
        "enrolled_at_percent_rank",
        "prev2_enrolled_at_3",
        "enrolled_at_percent_rank_2",
        (F.col("rn") + F.col("enrolled_at_percent_rank")).alias("rn_plus_enrolled_at_percent_rank"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev2_enrolled_at_3")).alias("days_since_prev2_enrolled_at_3"),
    )
    .orderBy(
        F.col("id").desc_nulls_first(),
        F.col("last_known_enrolled_at").desc_nulls_first(),
    )
    .limit(20)
)
