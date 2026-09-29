from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("score_tier", "course_id").orderBy("score", "id")
w_dedup = Window.partitionBy("student_id", "score_tier").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

recent_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.when(F.col("score") >= 59.7, "A").when(F.col("score") >= 20.2, "B").otherwise("C").alias("score_tier"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
)

unique_rows = (
    recent_enrollments
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

unique_rows_2 = (
    unique_rows
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows_2
    .groupBy("course_id")
    .pivot("score_tier", ["C", "A", "B"])
    .agg(F.max("score"))
    .orderBy("course_id")
)
