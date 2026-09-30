from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_student_id = Window.partitionBy("term", "student_id")
w_grade_status_student_id_by_s = Window.partitionBy("grade_status", "student_id").orderBy("score_bucket", "id")

cleaned = (
    df_enrollments
    .filter(F.col("grade_status").isin("WITHDRAWN", "PASS", "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("FAIL", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

scored = (
    cleaned
    .filter(F.col("score") <= 66.0)
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "score_bucket",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

ranked = (
    scored
    .select(
        "*",
        F.round(F.col("score_bucket") - F.avg("score_bucket").over(w_term_student_id), 2).alias("score_bucket_vs_term_student_id_avg"),
        F.row_number().over(w_grade_status_student_id_by_s).alias("score_bucket_seq"),
        F.lead("enrolled_at").over(w_grade_status_student_id_by_s).alias("next_enrolled_at"),
    )
)

enriched = ranked.withColumn("term_len", F.length(F.trim(F.col("term"))))

result = (
    enriched
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "term_slug",
        "score_bucket_vs_term_student_id_avg",
        "next_enrolled_at",
        "term_len",
        (F.col("term_len") + F.col("score_bucket")).alias("term_len_plus_score_bucket"),
        F.when(F.col("term").isin("2024T1", "2024T2"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
    )
    .orderBy(F.col("term").asc(), "term_slug")
)
