from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_or_unknown_student_id_b = Window.partitionBy("term_or_unknown", "student_id").orderBy("enrolled_at_date", "id")
w_course_id = Window.partitionBy("course_id")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

ranked = (
    filtered_enrollments
    .select(
        "*",
        F.lag("enrolled_at", 3).over(w_term_or_unknown_student_id_b).alias("prev3_enrolled_at"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.rank().over(w_term_or_unknown_student_id_b).alias("enrolled_at_date_rank"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
    )
)

result = (
    ranked
    .cube("student_id", "enrolled_at_date_rank", "course_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.round(F.sum("score_vs_course_id_avg") / F.nullif(F.sum("enrolled_at_date_rank"), F.lit(0)), 3).alias("score_vs_course_id_avg_to_enrolled_at_date_rank_"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("enrolled_at_date_rank").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
