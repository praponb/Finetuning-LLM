from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("term").orderBy(F.col("score").desc(), F.col("id").desc())

recent = (
    df_enrollments
    .filter(F.col("course_id") >= 2)
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

unique_rows = (
    recent
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

derived = (
    unique_rows
    .select(
        "*",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
        (F.col("enrolled_at") + F.expr("INTERVAL 7 DAYS")).alias("enrolled_at_plus_7d"),
    )
)

result = (
    derived
    .groupingSets(
        [["grade_status", "course_id"], ["grade_status"], ["course_id"], []],
        "grade_status", "course_id",
    )
    .agg(F.countDistinct("id").alias("distinct_ids"))
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
