from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows = Window.partitionBy("n_rows")
w_all_by_n_term_2025t2 = Window.orderBy("n_term_2025t2", "grade_status")

valid_enrollments = (
    df_enrollments
    .filter((F.col("course_id") >= 2) | (F.col("grade_status").isin("INCOMPLETE", "FAIL")))
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

enriched_course = (
    valid_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select("s.*", "c.course_code", F.col("c.dept").alias("course_dept"))
)

rollup_base = (
    enriched_course
    .filter(F.col("student_id").isNotNull())
    .groupBy("grade_status")
    .agg(
        F.count(F.when(F.col("term") != "2025T2", 1)).alias("n_term_2025t2"),
        F.count("*").alias("n_rows"),
        F.sum("score").alias("total_score"),
    )
)

annotated = (
    rollup_base
    .select(
        "grade_status",
        "n_term_2025t2",
        "n_rows",
        (F.floor(F.col("n_term_2025t2") / 2) * 2).alias("n_term_2025t2_bucket"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        (F.col("total_score") - F.col("n_term_2025t2")).alias("total_score_minus_n_term_2025t2"),
    )
)

filtered = (
    annotated
    .crossJoin(annotated.agg(F.min("grade_status_len").alias("overall_min_grade_status_len")))
    .filter(F.col("grade_status_len") > F.col("overall_min_grade_status_len"))
    .drop("overall_min_grade_status_len")
)

ranked = (
    filtered
    .withColumn("n_rows_sum_n_term_2025t2", F.sum("n_term_2025t2").over(w_n_rows))
    .withColumn("prev_n_term_2025t2_bucket", F.lag("n_term_2025t2_bucket").over(w_all_by_n_term_2025t2))
)

result = (
    ranked
    .select(
        "grade_status",
        "n_term_2025t2",
        "n_rows",
        "n_term_2025t2_bucket",
        "grade_status_len",
        "total_score_minus_n_term_2025t2",
        "n_rows_sum_n_term_2025t2",
        "prev_n_term_2025t2_bucket",
    )
    .orderBy(F.col("n_rows_sum_n_term_2025t2").desc())
)
