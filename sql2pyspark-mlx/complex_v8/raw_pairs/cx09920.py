from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_course_credits_runnin = Window.orderBy("course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status = Window.partitionBy("grade_status")
w_term_grade_status_by_student = Window.partitionBy("term", "grade_status").orderBy(F.col("student_enrolled_on").desc(), "id")
w_all_by_student_enrolled_on = Window.orderBy("student_enrolled_on", "id")
w_course_credits_by_enrolled_a = Window.partitionBy("course_credits").orderBy("enrolled_at", "id")
w_all_by_course_credits_full = Window.orderBy("course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_credits_by_enrolled_a_2 = Window.partitionBy("course_credits").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_month_course_cre = Window.partitionBy(F.month("enrolled_at"), "course_credits")
w_course_credits_by_enrolled_a_3 = Window.partitionBy("course_credits").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_next3_course_credi = Window.partitionBy("course_id", "next3_course_credits")
w_term_by_second_score = Window.partitionBy("term").orderBy("second_score", "id")
w_course_id_by_enrolled_at_ful = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_enrollments
    .filter((F.col("course_id") <= 7) & (F.hour(F.col("enrolled_at")).between(7, 12)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

combined_details_course = (
    scoped.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.lower(F.col("c.course_code")).like("%i3%")), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

metrics = (
    combined_details_course
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "score_bucket",
        "grade_status_clean",
        "course_credits",
        F.min("score_bucket").over(w_all_by_course_credits_runnin).alias("min_score_bucket_so_far"),
        F.round(F.col("score_bucket") - F.avg("score_bucket").over(w_grade_status), 2).alias("score_bucket_vs_grade_status_avg"),
        F.round(F.percent_rank().over(w_term_grade_status_by_student), 3).alias("student_enrolled_on_percent_rank"),
        F.dense_rank().over(w_all_by_student_enrolled_on).alias("student_enrolled_on_dense_rank"),
        F.lead("score_bucket", 3, 0).over(w_course_credits_by_enrolled_a).alias("next3_score_bucket"),
        F.lag("score_bucket", 1, 0).over(w_term_grade_status_by_student).alias("prev_score_bucket"),
        F.nth_value("course_credits", 2).over(w_all_by_course_credits_full).alias("second_course_credits"),
        F.round(F.avg("score_bucket").over(w_grade_status), 2).alias("grade_status_avg_score_bucket"),
        F.last("score_bucket", ignorenulls=True).over(w_course_credits_by_enrolled_a_2).alias("last_known_score_bucket"),
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_grade_status), F.lit(0)), 4).alias("course_credits_share_of_grade_status"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_month_course_cre)).alias("distinct_course_id_in_enrolled_at_month_course_c"),
        F.round(F.col("score") - F.avg("score").over(w_enrolled_at_month_course_cre), 2).alias("score_vs_enrolled_at_month_course_credits_avg"),
        F.lead("course_credits", 3).over(w_all_by_student_enrolled_on).alias("next3_course_credits"),
        F.nth_value("score", 2).over(w_course_credits_by_enrolled_a_3).alias("second_score"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
    )
)

top_per_group = (
    metrics
    .filter((F.col("student_enrolled_on_dense_rank") == 1) & (F.col("distinct_course_id_in_enrolled_at_month_course_c") >= 4))
    .drop("student_enrolled_on_dense_rank")
)

ranked = (
    top_per_group
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        "grade_status_clean",
        "course_credits",
        "min_score_bucket_so_far",
        "score_bucket_vs_grade_status_avg",
        "student_enrolled_on_percent_rank",
        "next3_score_bucket",
        "last_known_score_bucket",
        "distinct_course_id_in_enrolled_at_month_course_c",
        "next3_course_credits",
        "second_score",
        F.round(F.avg("score_bucket").over(w_course_id_next3_course_credi), 2).alias("course_id_next3_course_credits_avg_score_bucket"),
        (F.col("score") - F.lag("score").over(w_term_by_second_score)).alias("score_change"),
        F.size(F.collect_set("term").over(w_course_id_next3_course_credi)).alias("distinct_term_in_course_id_next3_course_credits"),
        F.nth_value("next3_score_bucket", 2).over(w_course_id_by_enrolled_at_ful).alias("second_next3_score_bucket"),
    )
)

result = (
    ranked
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        "course_credits",
        "min_score_bucket_so_far",
        "score_bucket_vs_grade_status_avg",
        "student_enrolled_on_percent_rank",
        "next3_score_bucket",
        "distinct_course_id_in_enrolled_at_month_course_c",
        "next3_course_credits",
        "second_score",
        "distinct_term_in_course_id_next3_course_credits",
    )
    .orderBy(F.col("distinct_term_in_course_id_next3_course_credits").desc())
)
