from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_score = Window.partitionBy("student_id").orderBy("score", "id")
w_student_id_by_score_running = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("term").isNotNull()) & (F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE", "FAIL")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_trends = (
    cleaned_enrollments
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.dense_rank().over(w_student_id_by_score).alias("score_dense_rank"),
        F.first("score", ignorenulls=True).over(w_student_id_by_score_running).alias("first_known_score"),
    )
)

deduped = (
    with_trends
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.min("score").alias("min_enrollment_score"))
)

with_stats = (
    deduped.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.min_enrollment_score")
)

result = (
    with_stats
    .select(
        "id",
        F.col("score").alias("m_score"),
        F.col("min_enrollment_score").alias("min_enrollment_score_v"),
        F.col("first_known_score").alias("m_first_known_score"),
    )
    .unpivot(
        ids=["id"],
        values=["m_score", "min_enrollment_score_v", "m_first_known_score"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "field")
)
