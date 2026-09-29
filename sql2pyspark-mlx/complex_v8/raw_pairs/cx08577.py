from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_n_grade_status = Window.partitionBy("course_id").orderBy("n_grade_status_incomplete", "course_id")
w_distinct_student_ids_course = Window.partitionBy("distinct_student_ids", "course_id").orderBy("n_rows_change", "course_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_n_grade_status_inc = Window.partitionBy("course_id", "n_grade_status_incomplete_tercile")
w_all_by_max_score = Window.orderBy("max_score", "course_id")

valid = (
    df_enrollments
    .filter((F.col("score") <= 79.2) | (F.month(F.col("enrolled_at")).isin(1, 2)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

grouped = (
    valid
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("grade_status") == "INCOMPLETE", 1)).alias("n_grade_status_incomplete"),
        F.count("*").alias("n_rows"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.max("score").alias("max_score"),
        F.avg("score").alias("avg_score_check"),
    )
    .filter(F.col("avg_score_check") > 10.8)
    .drop("avg_score_check")
)

with_trends = (
    grouped
    .select(
        "*",
        (F.col("n_rows") - F.lag("n_rows").over(w_course_id_by_n_grade_status)).alias("n_rows_change"),
        F.ntile(3).over(w_course_id_by_n_grade_status).alias("n_grade_status_incomplete_tercile"),
    )
)

metrics = (
    with_trends
    .select(
        "course_id",
        "n_rows",
        "distinct_student_ids",
        "max_score",
        "n_rows_change",
        "n_grade_status_incomplete_tercile",
        F.last("n_rows_change", ignorenulls=True).over(w_distinct_student_ids_course).alias("last_known_n_rows_change"),
        F.round(F.col("n_grade_status_incomplete") / F.nullif(F.sum("n_grade_status_incomplete").over(w_course_id_n_grade_status_inc), F.lit(0)), 4).alias("n_grade_status_incomplete_share_of_course_id_n_g"),
        F.lead("max_score", 2).over(w_all_by_max_score).alias("next2_max_score"),
        F.count(F.lit(1)).over(w_distinct_student_ids_course).alias("running_count"),
    )
)

result = (
    metrics
    .select(
        "course_id",
        "n_rows",
        "distinct_student_ids",
        "max_score",
        "n_grade_status_incomplete_tercile",
        "last_known_n_rows_change",
        "n_grade_status_incomplete_share_of_course_id_n_g",
        "running_count",
        F.abs(F.col("max_score") - 65.0).alias("max_score_distance"),
    )
    .orderBy(
        F.col("max_score_distance").asc_nulls_last(),
        F.col("last_known_n_rows_change").desc(),
    )
)
