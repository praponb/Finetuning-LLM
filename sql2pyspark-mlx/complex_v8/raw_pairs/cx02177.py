from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_course_id_student_id_by_scor = Window.partitionBy("course_id", "student_id").orderBy("score", "id").rowsBetween(-6, -1)
w_course_id_student_id_by_scor_2 = Window.partitionBy("course_id", "student_id").orderBy("score")

cleaned = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.to_date(F.col("enrolled_at")) < F.lit("2025-02-06").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.round(F.avg("score").over(w_course_id_student_id_by_scor), 2).alias("score_avg_6_prior"),
        F.round(F.cume_dist().over(w_course_id_student_id_by_scor_2), 3).alias("score_cume_dist"),
    )
)

course_id_stats = (
    ranked
    .groupBy("course_id")
    .agg(F.avg("score").alias("avg_score"))
)

outliers = (
    ranked.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

with_activity = (
    outliers.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status").isin("WITHDRAWN", "INCOMPLETE")), "left_semi")
    .filter(F.hour(F.col("enrolled_at_quarter")).between(7, 11))
)

result = (
    with_activity
    .select(
        "id",
        "score_share_of_course_id",
        "score",
        "avg_score",
    )
    .unpivot(
        ids=["id"],
        values=["score_share_of_course_id", "score", "avg_score"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric_name")
)
