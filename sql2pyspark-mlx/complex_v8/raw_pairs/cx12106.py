from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_grade_status_coun = Window.partitionBy("enrollment_grade_status_count", F.date_trunc("month", "last_enrollment_enrolled_at"))
w_student_id_by_course_credits = Window.partitionBy("student_id").orderBy("course_credits", "id")
w_enrollment_grade_status_coun_2 = Window.partitionBy("enrollment_grade_status_count").orderBy("enrollment_count", "id")
w_enrollment_grade_status_coun_3 = Window.partitionBy("enrollment_grade_status_count").orderBy("enrollment_count")
w_enrollment_grade_status_coun_4 = Window.partitionBy("enrollment_grade_status_count").orderBy("enrollment_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_len_by_score_adj = Window.partitionBy("term_len").orderBy(F.col("score_adj").desc_nulls_first(), "id")
w_all_by_course_credits = Window.orderBy(F.col("course_credits").desc(), "id")
w_enrollment_grade_status_coun_5 = Window.partitionBy("enrollment_grade_status_count").orderBy(F.col("score").desc())
w_enrollment_count_last_enroll = Window.partitionBy("enrollment_count", F.year("last_enrollment_enrolled_at"))
w_all_by_course_credits_ties = Window.orderBy(F.col("course_credits").desc())
w_last_last_enrollment_enrolle = Window.partitionBy(F.year("last_last_enrollment_enrolled_at")).orderBy(F.col("score").desc(), "id").rowsBetween(-2, -1)

scoped_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("FAIL", "PASS")) & (F.col("score") > 42.4))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.concat_ws("-", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

profiled = (
    scoped_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("grade_status") != "PASS", 1)).alias("enrollment_grade_status_count"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

with_stats = (
    profiled.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.last_enrollment_enrolled_at",
    )
)

joined = (
    with_stats.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at_label",
        "s.enrollment_count",
        "s.enrollment_grade_status_count",
        "s.last_enrollment_enrolled_at",
        F.col("c.credits").alias("course_credits"),
    )
)

derived = (
    joined
    .select(
        "*",
        F.round(F.col("score") * 1.5, 2).alias("score_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

with_windows = (
    derived
    .select(
        "*",
        F.min("score_adj").over(w_enrollment_grade_status_coun).alias("enrollment_grade_status_count_last_enrollment_en"),
        F.lead("course_credits", 3).over(w_student_id_by_course_credits).alias("next3_course_credits"),
        (F.col("enrollment_count") - F.lag("enrollment_count").over(w_enrollment_grade_status_coun_2)).alias("enrollment_count_change"),
        F.dense_rank().over(w_enrollment_grade_status_coun_3).alias("enrollment_count_dense_rank"),
        F.last("last_enrollment_enrolled_at").over(w_enrollment_grade_status_coun_4).alias("last_last_enrollment_enrolled_at"),
        F.lead("course_credits", 2).over(w_term_len_by_score_adj).alias("next2_course_credits"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrollment_count",
        "enrollment_grade_status_count",
        "last_enrollment_enrolled_at",
        "course_credits",
        "score_adj",
        "term_len",
        "next3_course_credits",
        "enrollment_count_change",
        "enrollment_count_dense_rank",
        "next2_course_credits",
        F.ntile(10).over(w_all_by_course_credits).alias("course_credits_decile"),
        F.rank().over(w_enrollment_grade_status_coun_5).alias("score_rank"),
        F.count(F.lit(1)).over(w_enrollment_count_last_enroll).alias("rows_in_enrollment_count_last_enrollment_enrolle"),
        F.round(F.cume_dist().over(w_all_by_course_credits_ties), 3).alias("course_credits_cume_dist"),
        F.sum("score_adj").over(w_last_last_enrollment_enrolle).alias("score_adj_sum_2_prior"),
        F.lead("next3_course_credits", 2).over(w_all_by_course_credits).alias("next2_next3_course_credits"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "student_id",
        "score",
        "enrollment_count",
        "enrollment_grade_status_count",
        "last_enrollment_enrolled_at",
        "course_credits",
        "score_adj",
        "term_len",
        "next3_course_credits",
        "enrollment_count_change",
        "enrollment_count_dense_rank",
        "course_credits_decile",
        "score_rank",
        "rows_in_enrollment_count_last_enrollment_enrolle",
        "course_credits_cume_dist",
        "score_adj_sum_2_prior",
        "next2_next3_course_credits",
        F.round(F.col("course_credits_cume_dist") / F.nullif(F.col("course_credits"), F.lit(0)), 3).alias("course_credits_cume_dist_per_course_credits"),
    )
    .orderBy(
        F.col("enrollment_count_change").asc_nulls_last(),
        F.col("score_adj_sum_2_prior").asc_nulls_last(),
        "id",
    )
    .limit(20)
)
