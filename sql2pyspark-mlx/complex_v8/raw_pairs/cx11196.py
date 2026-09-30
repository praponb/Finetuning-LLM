from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_to_date_by_enrol = Window.partitionBy(F.to_date("enrolled_at")).orderBy(F.col("enrolled_at").desc())
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")

base = (
    df_enrollments
    .filter((F.col("student_id") >= 12) & (F.col("course_id") <= 11))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

windowed = (
    base
    .withColumn("enrolled_at_rank", F.rank().over(w_enrolled_at_to_date_by_enrol))
    .withColumn("score_share_of_student_id_grade_status", F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_grade_status), F.lit(0)), 4))
)

result = (
    windowed
    .groupingSets(
        [["course_id", "student_id", "grade_status"], ["course_id"], []],
        "course_id", "student_id", "grade_status",
    )
    .agg(
        F.min("score").alias("min_score"),
        F.round(100 * F.count(F.when(F.col("grade_status") == "FAIL", 1)) / F.count("*"), 1).alias("pct_grade_status_fail"),
        F.round(100 * F.count(F.when(F.col("grade_status").isin("WITHDRAWN", "FAIL", "PASS"), 1)) / F.count("*"), 1).alias("pct_grade_status_in_withdrawn_fa"),
    )
    .select(
        "course_id",
        "student_id",
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "min_score",
        "pct_grade_status_fail",
        "pct_grade_status_in_withdrawn_fa",
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
