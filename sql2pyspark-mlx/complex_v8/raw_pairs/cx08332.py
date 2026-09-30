from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_date = Window.orderBy("enrolled_at_date", "id")
w_all_by_enrolled_at_date_prio = Window.orderBy("enrolled_at_date", "id").rowsBetween(-1, -1)

valid_enrollments = (
    df_enrollments
    .filter((F.hour(F.col("enrolled_at")).between(10, 12)) & (F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2025T2", "2025T1"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

annotated = (
    valid_enrollments
    .select(
        "*",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

sequenced = (
    annotated
    .withColumn("enrolled_at_date_percent_rank", F.round(F.percent_rank().over(w_all_by_enrolled_at_date), 3))
    .withColumn("score_filled_max_1_prior", F.max("score_filled").over(w_all_by_enrolled_at_date_prio))
)

combined_details = (
    sequenced.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_group",
        "s.term_slug",
        "s.grade_status_prefix",
        "s.score_filled",
        "s.enrolled_at_date_percent_rank",
        "s.score_filled_max_1_prior",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
    )
)

without_matches = (
    combined_details.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.enrolled_on").isNotNull()) & (F.col("st.gpa") > 2.1), "left_anti")
)

filtered = (
    without_matches.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") <= 79.3) & (F.col("e.term") == "2024T2"), "left_anti")
)

course_id_stats = (
    filtered
    .groupBy("course_id")
    .agg(F.avg("score_filled").alias("avg_score_filled"))
)

above_average = (
    filtered.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score_filled") > F.col("g.avg_score_filled"))
    .select(
        "s.*",
        "g.avg_score_filled",
        F.round(F.col("s.score_filled") - F.col("g.avg_score_filled"), 2).alias("score_filled_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "term_group",
        "term_slug",
        "grade_status_prefix",
        "score_filled",
        "enrolled_at_date_percent_rank",
        "score_filled_max_1_prior",
        "course_credits",
        "score_filled_minus_avg",
    )
    .orderBy("grade_status")
)
