from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_term_by_enrolled_at_full = Window.partitionBy("term").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month_term_by_sc = Window.partitionBy(F.month("enrolled_at"), "term").orderBy(F.col("score_bucket").desc())
w_course_id_by_score_bucket_fu = Window.partitionBy("course_id").orderBy("score_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_course_id_by_enrolled_a = Window.partitionBy("term", "course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_month_term_by_sc_2 = Window.partitionBy(F.month("enrolled_at"), "term").orderBy(F.col("score_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term = Window.partitionBy("term")

valid = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) | (F.col("score").between(13.9, 59.7)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

sequenced = (
    valid
    .select(
        "*",
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.nth_value("score", 2).over(w_term_by_enrolled_at_full).alias("second_score"),
        F.rank().over(w_enrolled_at_month_term_by_sc).alias("score_bucket_rank"),
        F.round(F.col("score_bucket") - F.avg("score_bucket").over(w_student_id), 2).alias("score_bucket_vs_student_id_avg"),
        F.max("score").over(w_student_id).alias("student_id_max_score"),
        F.sum("score_bucket").over(w_student_id).alias("student_id_sum_score_bucket"),
        F.last("score_bucket").over(w_course_id_by_score_bucket_fu).alias("last_score_bucket"),
        F.last("score_bucket", ignorenulls=True).over(w_term_course_id_by_enrolled_a).alias("last_known_score_bucket"),
        F.last("score").over(w_enrolled_at_month_term_by_sc_2).alias("last_score"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term), F.lit(0)), 4).alias("score_share_of_term"),
    )
)

annotated = (
    sequenced
    .filter(F.col("student_id") <= 9)
    .select(
        "*",
        F.when(F.col("term").isin("2024T2", "2024T1"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
    )
)

result = (
    annotated
    .select("rows_in_term", "score_bucket", "id")
    .distinct()
    .orderBy(F.col("rows_in_term").desc(), "score_bucket")
)
