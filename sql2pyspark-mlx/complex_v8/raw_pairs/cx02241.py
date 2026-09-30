from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("score_tier").orderBy(F.col("avg_score").desc(), F.col("id").desc())

filtered_enrollments = (
    df_enrollments
    .filter((F.col("student_id") >= 9) | (F.col("term") == "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.when(F.col("score") >= 62.5, "large").when(F.col("score") >= 38.3, "medium").otherwise("small").alias("score_tier"),
    )
)

score_tier_stats = (
    filtered_enrollments
    .groupBy("score_tier")
    .agg(F.avg("score").alias("avg_score"))
)

compared = (
    filtered_enrollments.alias("s")
    .join(score_tier_stats.alias("g"), F.col("g.score_tier") == F.col("s.score_tier"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

latest = (
    compared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    latest.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .filter(F.length(F.trim(F.col("st.student_name"))) >= 4)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_term_label",
        "s.avg_score",
        "s.score_minus_avg",
        "st.student_name",
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    joined
    .select(
        "id",
        "score",
        "score_minus_avg",
        "avg_score",
    )
    .unpivot(
        ids=["id"],
        values=["score", "score_minus_avg", "avg_score"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .orderBy("id", "metric_name")
)
