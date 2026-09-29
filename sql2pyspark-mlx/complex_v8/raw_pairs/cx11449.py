from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_student_enrolled = Window.partitionBy("student_id", F.year("student_enrolled_on")).orderBy("student_enrolled_on")
w_student_id_student_enrolled_2 = Window.partitionBy("student_id", F.year("student_enrolled_on")).orderBy("student_enrolled_on", "id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_all_by_student_enrolled_on = Window.orderBy("student_enrolled_on", "id")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_last_student_enrolled_on_mon = Window.partitionBy(F.date_trunc("month", "last_student_enrolled_on")).orderBy("student_enrolled_on_percent_rank_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_enrolled_on_month = Window.partitionBy(F.date_trunc("month", "student_enrolled_on"))
w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_all_by_next_enrolled_at_runn = Window.orderBy(F.col("next_enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_enrollments
    .filter(F.col("score") < 80.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

joined = (
    valid.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.upper(F.trim(F.col("c.course_code"))).like("C%"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "c.course_code",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.major").alias("student_major"),
    )
)

windowed = (
    joined
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "student_enrolled_on",
        "student_major",
        F.round(F.cume_dist().over(w_student_id_student_enrolled), 3).alias("student_enrolled_on_cume_dist"),
        F.round(F.percent_rank().over(w_student_id_student_enrolled_2), 3).alias("student_enrolled_on_percent_rank"),
        F.lead("enrolled_at").over(w_all_by_enrolled_at).alias("next_enrolled_at"),
        F.round(F.percent_rank().over(w_student_id_student_enrolled), 3).alias("student_enrolled_on_percent_rank_2"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_student_enrolled_on))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.last("student_enrolled_on").over(w_all_by_enrolled_at_full).alias("last_student_enrolled_on"),
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank"),
    )
)

top_rows = (
    windowed
    .filter((F.col("enrolled_at_rank") <= 2) & (F.col("student_enrolled_on_cume_dist") < 0.7))
)

metrics = (
    top_rows
    .select(
        "*",
        F.max("enrolled_at_rank").over(w_last_student_enrolled_on_mon).alias("max_enrolled_at_rank_so_far"),
        F.round(F.col("enrolled_at_rank") - F.avg("enrolled_at_rank").over(w_student_enrolled_on_month), 2).alias("enrolled_at_rank_vs_student_enrolled_on_month_av"),
        F.round(F.col("enrolled_at_rank") / F.nullif(F.sum("enrolled_at_rank").over(w_student_id_course_id), F.lit(0)), 4).alias("enrolled_at_rank_share_of_student_id_course_id"),
        F.first("enrolled_at", ignorenulls=True).over(w_all_by_next_enrolled_at_runn).alias("first_known_enrolled_at"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "student_enrolled_on",
        "student_enrolled_on_cume_dist",
        "student_enrolled_on_percent_rank",
        "next_enrolled_at",
        "student_enrolled_on_percent_rank_2",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_rank",
        "max_enrolled_at_rank_so_far",
        "enrolled_at_rank_vs_student_enrolled_on_month_av",
        "enrolled_at_rank_share_of_student_id_course_id",
        "first_known_enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
    .orderBy(F.col("student_id").desc())
)
