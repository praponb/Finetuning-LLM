from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_dept_course_id = Window.partitionBy("course_dept", "course_id")

base = (
    df_enrollments
    .filter((F.col("score") < 78.8) & (F.col("enrolled_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.abs(F.col("score") - 57.6).alias("score_distance"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
    )
)

enriched = (
    base.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score_distance",
        "s.term_clean",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_course_dept_course_id), 2).alias("course_credits_vs_course_dept_course_id_avg"),
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_course_dept_course_id), F.lit(0)), 4).alias("score_distance_share_of_course_dept_course_id"),
        F.max("course_credits").over(w_course_dept_course_id).alias("course_dept_course_id_max_course_credits"),
        F.round(F.avg("score_distance").over(w_course_dept_course_id), 2).alias("course_dept_course_id_avg_score_distance"),
    )
)

result = (
    windowed
    .select("id", "course_dept_course_id_max_course_credits", "course_credits")
    .unpivot(
        ids=["id"],
        values=["course_dept_course_id_max_course_credits", "course_credits"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
