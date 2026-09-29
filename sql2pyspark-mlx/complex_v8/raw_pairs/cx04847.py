from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term")
w_term_by_score_bucket_running = Window.partitionBy("term").orderBy(F.col("score_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_score = Window.partitionBy("student_id").orderBy("score", "id")
w_term_by_enrolled_at = Window.partitionBy("term").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")

cleaned = (
    df_enrollments
    .filter((F.col("score") <= 80.2) | (~F.col("grade_status").isin("PASS", "FAIL")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

with_activity = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (~F.col("e.grade_status").isin("WITHDRAWN", "FAIL")), "left_semi")
)

without_matches = (
    with_activity.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.hour(F.col("e.enrolled_at")).between(10, 13)), "left_semi")
    .filter(F.col("student_id") <= 7)
)

ranked = (
    without_matches
    .select(
        "id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "term_clean",
        "enrolled_at_label",
        "score_bucket",
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.first("score", ignorenulls=True).over(w_term_by_score_bucket_running).alias("first_known_score"),
        (F.col("score") - F.lag("score").over(w_student_id_by_score)).alias("score_change"),
        F.lead("score").over(w_term_by_enrolled_at).alias("next_score"),
    )
)

result = (
    ranked
    .groupBy("enrolled_at")
    .pivot("grade_status", ["PASS", "INCOMPLETE", "FAIL", "WITHDRAWN"])
    .agg(F.sum("score_bucket"))
    .orderBy("enrolled_at")
)
