from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_running = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id = Window.partitionBy("course_id")
w_dedup = Window.partitionBy("course_dept", "term").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

filtered = (
    df_enrollments
    .filter(F.col("grade_status").isin("FAIL", "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

windowed = (
    filtered
    .select(
        "id",
        "course_id",
        "term",
        "enrolled_at",
        F.min("score").over(w_all_by_score_running).alias("min_score_so_far"),
        F.sum("score").over(w_course_id).alias("course_id_sum_score"),
    )
)

with_details = (
    windowed.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.min_score_so_far",
        "s.course_id_sum_score",
        F.col("c.dept").alias("course_dept"),
    )
)

unique_rows = (
    with_details
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .groupBy("term")
    .pivot("course_dept", ["Art", "Bio", "Math", "Biz"])
    .agg(F.avg("course_id_sum_score"))
)
