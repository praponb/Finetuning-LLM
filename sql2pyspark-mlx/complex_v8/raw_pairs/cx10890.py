from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_course_credits = Window.orderBy(F.col("course_credits").desc(), "id")
w_all_by_course_credits_full = Window.orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_plus_7d_year = Window.partitionBy(F.year("enrolled_at_plus_7d"))
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_course_credits_runnin = Window.orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_course_credits_by = Window.partitionBy("student_id", "course_credits").orderBy("enrolled_at_plus_7d", "id")
w_all_by_course_credits_2 = Window.orderBy("course_credits", "id")
w_prev3_enrolled_at_plus_7d_ye = Window.partitionBy(F.year("prev3_enrolled_at_plus_7d"), "grade_status")
w_all_by_prev3_enrolled_at_plu = Window.orderBy(F.col("prev3_enrolled_at_plus_7d").desc())

prepared = (
    df_enrollments
    .filter((F.col("grade_status") != "INCOMPLETE") | (F.col("term") != "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 7 DAYS")).alias("enrolled_at_plus_7d"),
    )
)

combined_details_course = (
    prepared.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_plus_7d",
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

ranked = (
    combined_details_course
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "course_credits",
        "course_level",
        F.lag("enrolled_at_plus_7d").over(w_all_by_course_credits).alias("prev_enrolled_at_plus_7d"),
        F.last("course_credits").over(w_all_by_course_credits_full).alias("last_course_credits"),
        F.lag("enrolled_at_plus_7d", 3).over(w_all_by_course_credits).alias("prev3_enrolled_at_plus_7d"),
        F.size(F.collect_set("term").over(w_enrolled_at_plus_7d_year)).alias("distinct_term_in_enrolled_at_plus_7d_year"),
        F.first("enrolled_at_plus_7d").over(w_grade_status_by_enrolled_at).alias("first_enrolled_at_plus_7d"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_plus_7d_year), F.lit(0)), 4).alias("score_share_of_enrolled_at_plus_7d_year"),
        F.max("course_credits").over(w_enrolled_at_plus_7d_year).alias("enrolled_at_plus_7d_year_max_course_credits"),
        F.lag("score", 2).over(w_all_by_course_credits).alias("prev2_score"),
        F.min("course_credits").over(w_all_by_course_credits_runnin).alias("min_course_credits_so_far"),
        F.ntile(3).over(w_student_id_course_credits_by).alias("enrolled_at_plus_7d_tercile"),
        F.round(F.avg("course_credits").over(w_enrolled_at_plus_7d_year), 2).alias("enrolled_at_plus_7d_year_avg_course_credits"),
        F.last("enrolled_at").over(w_grade_status_by_enrolled_at).alias("last_enrolled_at"),
    )
)

tagged = ranked.withColumn("course_credits_filled", F.coalesce(F.col("course_credits"), F.lit(0)))

with_windows = (
    tagged
    .select(
        "*",
        F.row_number().over(w_all_by_course_credits_2).alias("course_credits_seq"),
        F.sum("course_credits_filled").over(w_prev3_enrolled_at_plus_7d_ye).alias("prev3_enrolled_at_plus_7d_year_grade_status_sum_"),
        F.rank().over(w_all_by_prev3_enrolled_at_plu).alias("prev3_enrolled_at_plus_7d_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "course_credits",
        "last_course_credits",
        "prev3_enrolled_at_plus_7d",
        "distinct_term_in_enrolled_at_plus_7d_year",
        "first_enrolled_at_plus_7d",
        "score_share_of_enrolled_at_plus_7d_year",
        "prev2_score",
        "enrolled_at_plus_7d_tercile",
        "enrolled_at_plus_7d_year_avg_course_credits",
        "last_enrolled_at",
        "course_credits_filled",
        "course_credits_seq",
        "prev3_enrolled_at_plus_7d_year_grade_status_sum_",
        "prev3_enrolled_at_plus_7d_rank",
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
    .orderBy(F.col("student_id").asc(), F.col("course_credits_seq").desc(), "id")
    .limit(20)
)
