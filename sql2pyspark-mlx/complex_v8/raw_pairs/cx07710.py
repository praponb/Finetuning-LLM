from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("grade_status").orderBy(F.col("score_distance").desc(), F.col("id").desc())
w_course_id_student_id = Window.partitionBy("course_id", "student_id")
w_grade_status_student_id_by_e = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_dedup_2 = Window.partitionBy("student_id").orderBy(F.col("next_score_adj").desc(), F.col("id").desc())

cleaned = (
    df_enrollments
    .filter(F.col("score") >= 38.3)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.abs(F.col("score") - 59.8).alias("score_distance"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    deduped
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_filled",
        "score_adj",
        F.count(F.lit(1)).over(w_course_id_student_id).alias("rows_in_course_id_student_id"),
        F.lead("score_adj").over(w_grade_status_student_id_by_e).alias("next_score_adj"),
    )
)

enriched = (
    ranked
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_filled",
        "rows_in_course_id_student_id",
        "next_score_adj",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
        F.round(F.col("score") / F.nullif(F.col("score_filled"), F.lit(0)), 3).alias("score_per_score_filled"),
    )
)

latest = (
    enriched
    .withColumn("recency_rank", F.row_number().over(w_dedup_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select("id", "score_filled", "next_score_adj")
    .unpivot(
        ids=["id"],
        values=["score_filled", "next_score_adj"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
