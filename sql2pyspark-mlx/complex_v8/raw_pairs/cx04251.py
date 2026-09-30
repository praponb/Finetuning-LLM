from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at")
w_course_id_grade_status_by_en_2 = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_grade_status_by_en_3 = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id")

scoped_enrollments = (
    df_enrollments
    .filter((F.col("score") > 38.1) | (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
    )
)

sequenced = (
    scoped_enrollments
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        F.sum("score").over(w_course_id).alias("course_id_sum_score"),
        F.round(F.cume_dist().over(w_course_id_grade_status_by_en), 3).alias("enrolled_at_cume_dist"),
        F.last("score_adj").over(w_course_id_grade_status_by_en_2).alias("last_score_adj"),
        F.max("score_adj").over(w_course_id).alias("course_id_max_score_adj"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_id_grade_status_by_en_3))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.percent_rank().over(w_course_id_grade_status_by_en), 3).alias("enrolled_at_percent_rank"),
    )
)

filtered = (
    sequenced.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.cohort") != "C2024") & (F.col("st.gpa") >= 2.7), "left_semi")
)

student_id_stats = (
    filtered
    .groupBy("student_id")
    .agg(F.percentile_approx("enrolled_at_percent_rank", 0.5).alias("median_enrolled_at_percent_rank"))
)

vs_group = (
    filtered.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.enrolled_at_percent_rank") >= F.col("g.median_enrolled_at_percent_rank"))
    .select(
        "s.*",
        "g.median_enrolled_at_percent_rank",
        F.round(F.col("s.enrolled_at_percent_rank") - F.col("g.median_enrolled_at_percent_rank"), 2).alias("enrolled_at_percent_rank_minus_median"),
    )
)

enriched = (
    vs_group.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(~F.col("st.cohort").isin("C2021", "C2022"))
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.course_id_sum_score",
        "s.enrolled_at_cume_dist",
        "s.last_score_adj",
        "s.course_id_max_score_adj",
        "s.minutes_since_prev_enrolled_at",
        "s.enrolled_at_percent_rank",
        "s.median_enrolled_at_percent_rank",
        F.col("st.cohort").alias("student_cohort"),
    )
)

joined = (
    enriched.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.last_score_adj",
        "s.enrolled_at_percent_rank",
        "s.median_enrolled_at_percent_rank",
        "s.student_cohort",
        "st.student_name",
    )
)

result = (
    joined
    .filter(F.col("median_enrolled_at_percent_rank") >= 0.3)
    .select(
        "id",
        "student_id",
        "enrolled_at_percent_rank",
        "median_enrolled_at_percent_rank",
        "student_cohort",
        "student_name",
    )
    .orderBy(F.col("enrolled_at_percent_rank").desc_nulls_first())
)
