from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_major = Window.partitionBy("student_major")
w_all_by_enrolled_at_moving_6 = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)

base = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.5, 2).alias("score_adj"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

without_matches = (
    base.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") >= 2), "left_semi")
)

grade_status_stats = (
    without_matches
    .groupBy("grade_status")
    .agg(F.percentile_approx("score_adj", 0.5).alias("median_score_adj"))
)

compared = (
    without_matches.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score_adj") >= F.col("g.median_score_adj"))
    .select(
        "s.*",
        "g.median_score_adj",
        F.round(F.col("s.score_adj") - F.col("g.median_score_adj"), 2).alias("score_adj_minus_median"),
    )
)

with_details = (
    compared.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.is_international"))
    .select(
        "s.*",
        F.col("st.major").alias("student_major"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

enriched_student = (
    with_details.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_adj",
        "s.enrolled_at_label",
        "s.median_score_adj",
        "s.student_major",
        "s.student_is_international",
        "s.student_enrolled_on",
        F.col("st.cohort").alias("student_cohort"),
    )
)

sequenced = (
    enriched_student
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "score_adj",
        "enrolled_at_label",
        "median_score_adj",
        "student_major",
        "student_is_international",
        "student_enrolled_on",
        F.size(F.collect_set("student_id").over(w_student_major)).alias("distinct_student_id_in_student_major"),
        F.sum("score_adj").over(w_student_major).alias("student_major_sum_score_adj"),
        F.round(F.avg("score_adj").over(w_all_by_enrolled_at_moving_6), 2).alias("score_adj_avg_7"),
        F.min("score_adj").over(w_student_major).alias("student_major_min_score_adj"),
    )
)

labeled = (
    sequenced
    .select(
        "*",
        F.coalesce(F.col("student_major"), F.lit("UNKNOWN")).alias("student_major_or_unknown"),
        F.length(F.trim(F.col("student_major"))).alias("student_major_len"),
    )
)

result = (
    labeled
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "score_adj",
        "enrolled_at_label",
        "median_score_adj",
        "student_major",
        "student_is_international",
        "student_enrolled_on",
        "distinct_student_id_in_student_major",
        "student_major_sum_score_adj",
        "score_adj_avg_7",
        "student_major_min_score_adj",
        "student_major_or_unknown",
        "student_major_len",
    )
    .orderBy(F.col("student_major").asc_nulls_last(), "id")
    .limit(10)
)
