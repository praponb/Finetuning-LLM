from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_credits = Window.partitionBy("course_credits")
w_course_id_by_course_credits = Window.partitionBy("course_id").orderBy("course_credits", "id")
w_dedup = Window.partitionBy("student_id", "grade_status").orderBy(F.col("course_credits").desc(), F.col("id").desc())

base_enrollments = (
    df_enrollments
    .filter((F.col("score") > 38.1) & (F.col("enrolled_at") < F.lit("2025-03-02 08:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

joined = (
    base_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.course_code").isNotNull()), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

sequenced = (
    joined
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "course_credits",
        "course_level",
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_course_credits), F.lit(0)), 4).alias("course_credits_share_of_course_credits"),
        F.rank().over(w_course_id_by_course_credits).alias("course_credits_rank"),
        F.round(F.col("score") - F.avg("score").over(w_course_credits), 2).alias("score_vs_course_credits_avg"),
        F.round(F.avg("course_credits").over(w_course_credits), 2).alias("course_credits_avg_course_credits"),
    )
)

unique_rows = (
    sequenced
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select("id", "course_credits_rank", "course_credits")
    .unpivot(
        ids=["id"],
        values=["course_credits_rank", "course_credits"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
