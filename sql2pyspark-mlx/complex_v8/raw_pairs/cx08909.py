from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_enrolled_at_ties = Window.partitionBy("term").orderBy("enrolled_at")
w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_course_id_term_by_enrolled_a = Window.partitionBy("course_id", "term").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_course_id = Window.partitionBy("course_id")
w_all_by_enrolled_at_decile = Window.orderBy(F.col("enrolled_at_decile").desc(), "id")
w_term_enrolled_at_decile_by_f = Window.partitionBy("term", "enrolled_at_decile").orderBy(F.col("first_known_enrolled_at").desc(), "id")

base = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(9, 12))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 57.6, "large").when(F.col("score") >= 36.6, "medium").otherwise("small").alias("score_tier"),
    )
)

windowed = (
    base
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score_tier",
        F.rank().over(w_term_by_enrolled_at_ties).alias("enrolled_at_rank"),
        F.row_number().over(w_term_by_enrolled_at).alias("seq_no"),
        F.first("enrolled_at", ignorenulls=True).over(w_course_id_term_by_enrolled_a).alias("first_known_enrolled_at"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
        F.ntile(10).over(w_all_by_enrolled_at).alias("enrolled_at_decile"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at_rank",
        "first_known_enrolled_at",
        "enrolled_at_percent_rank",
        "enrolled_at_decile",
        F.size(F.collect_set("student_id").over(w_course_id)).alias("distinct_student_id_in_course_id"),
        (F.col("enrolled_at_percent_rank") - F.lag("enrolled_at_percent_rank").over(w_all_by_enrolled_at_decile)).alias("enrolled_at_percent_rank_change"),
        F.lead("seq_no").over(w_term_enrolled_at_decile_by_f).alias("next_seq_no"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_decile), 3).alias("enrolled_at_decile_cume_dist"),
        F.size(F.collect_set("score_tier").over(w_course_id)).alias("distinct_score_tier_in_course_id"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "term",
        "enrolled_at_rank",
        "first_known_enrolled_at",
        "enrolled_at_percent_rank",
        "enrolled_at_decile",
        "enrolled_at_percent_rank_change",
        "next_seq_no",
        "enrolled_at_decile_cume_dist",
        "distinct_score_tier_in_course_id",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_known_enrolled_at")).alias("days_since_first_known_enrolled_at"),
    )
    .orderBy(F.col("id").desc())
)
