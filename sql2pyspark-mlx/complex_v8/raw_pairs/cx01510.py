from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_filled_running = Window.orderBy(F.col("score_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_all_by_score_filled_full = Window.orderBy(F.col("score_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy(F.col("score_filled_2").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_student_id_by_score_ties = Window.partitionBy("student_id").orderBy(F.col("score").desc())
w_student_id_course_id_by_scor_2 = Window.partitionBy("student_id", "course_id").orderBy(F.col("score_filled_2").asc_nulls_last(), "id")

filtered = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled_2"),
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.min("score_filled_2").over(w_all_by_score_filled_running).alias("min_score_filled_2_so_far"),
        F.count(F.lit(1)).over(w_student_id_grade_status).alias("rows_in_student_id_grade_status"),
        F.first("score_filled").over(w_all_by_score_filled_full).alias("first_score_filled"),
        F.round(F.avg("score_filled").over(w_student_id_course_id_by_scor), 2).alias("score_filled_avg_4"),
        F.rank().over(w_student_id_by_score_ties).alias("score_rank"),
        F.ntile(5).over(w_student_id_course_id_by_scor_2).alias("score_filled_2_quintile"),
        F.round(F.col("score_filled") - F.avg("score_filled").over(w_student_id_grade_status), 2).alias("score_filled_vs_student_id_grade_status_avg"),
    )
)

top_rows = sequenced.filter(F.col("score_rank") <= 5)

result = (
    top_rows
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "score_filled_2",
        "min_score_filled_2_so_far",
        "rows_in_student_id_grade_status",
        "first_score_filled",
        "score_filled_avg_4",
        "score_rank",
        "score_filled_2_quintile",
        F.when(F.col("score_filled_2") >= 65.0, "large").when(F.col("score_filled_2") >= 30.0, "medium").otherwise("small").alias("score_filled_2_tier"),
    )
    .orderBy(F.col("rows_in_student_id_grade_status").asc_nulls_last())
)
