from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())
w_latest_2 = Window.partitionBy("student_id").orderBy("score", "id")

prepared_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("|", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

unique_rows = (
    prepared_enrollments
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

latest_per_key = (
    unique_rows
    .withColumn("rn", F.row_number().over(w_latest_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = latest_per_key.withColumn("score_adj", F.round(F.col("score") * 1.5, 2))

result = (
    scored
    .cube("student_id", "course_id")
    .agg(
        F.grouping("student_id").alias("is_total_student_id"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.round(F.sum("score_adj") / F.nullif(F.sum("score"), F.lit(0)), 3).alias("score_adj_to_score_ratio"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
    )
)
