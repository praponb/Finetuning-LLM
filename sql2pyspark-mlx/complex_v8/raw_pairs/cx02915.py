from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_student_id_by_scor = Window.partitionBy("course_id", "student_id").orderBy(F.col("score_adj").desc(), "id")
w_enrolled_at_year = Window.partitionBy(F.year("enrolled_at"))
w_dedup = Window.partitionBy("course_id", "student_id").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())
w_latest = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

recent = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(": ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
    )
)

with_trends = (
    recent
    .withColumn("score_adj_cume_dist", F.round(F.cume_dist().over(w_course_id_student_id_by_scor), 3))
    .withColumn("rows_in_enrolled_at_year", F.count(F.lit(1)).over(w_enrolled_at_year))
)

unique_rows = (
    with_trends
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

unique_rows_2 = (
    unique_rows
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows_2
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "grade_status_term_label",
        "grade_status_ref",
        "score_adj_cume_dist",
        "rows_in_enrolled_at_year",
    )
    .orderBy(F.col("grade_status_ref").desc(), "enrolled_at")
)
