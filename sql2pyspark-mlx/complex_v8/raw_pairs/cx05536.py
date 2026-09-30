from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_id = Window.partitionBy("term", "course_id")
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy("score", "id")
w_student_id_term = Window.partitionBy("student_id", "term")
w_student_id_course_id_by_scor_2 = Window.partitionBy("student_id", "course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_course_id_by_scor_3 = Window.partitionBy("student_id", "course_id").orderBy("score")
w_course_id_term_by_enrolled_a = Window.partitionBy("course_id", "term").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_term_by_enrolled_a_2 = Window.partitionBy("course_id", "term").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_enrolled_at_month = Window.partitionBy(F.month("enrolled_at"))
w_term_by_next_enrolled_at_2 = Window.partitionBy("term").orderBy("next_enrolled_at_2", "id")
w_term_enrolled_at_month_by_te = Window.partitionBy("term", F.month("enrolled_at")).orderBy(F.col("term_course_id_min_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_enrolled_at_month_by_te_2 = Window.partitionBy("term", F.month("enrolled_at")).orderBy(F.col("term_course_id_min_score").desc())

scoped = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("term") == "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

ranked = (
    scoped
    .select(
        "id",
        "term",
        "score",
        "enrolled_at",
        F.min("score").over(w_term_course_id).alias("term_course_id_min_score"),
        F.ntile(3).over(w_student_id_course_id_by_scor).alias("score_tercile"),
        F.round(F.col("score") - F.avg("score").over(w_student_id_term), 2).alias("score_vs_student_id_term_avg"),
        F.last("score").over(w_student_id_course_id_by_scor_2).alias("last_score"),
        F.rank().over(w_student_id_course_id_by_scor_3).alias("score_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term_course_id), F.lit(0)), 4).alias("score_share_of_term_course_id"),
        F.lead("enrolled_at").over(w_course_id_term_by_enrolled_a).alias("next_enrolled_at"),
        F.nth_value("score", 2).over(w_all_by_enrolled_at_full).alias("second_score"),
        F.last("enrolled_at", ignorenulls=True).over(w_course_id_term_by_enrolled_a_2).alias("last_known_enrolled_at"),
        F.lead("enrolled_at", 3).over(w_student_id_by_enrolled_at).alias("next3_enrolled_at"),
        F.lead("enrolled_at").over(w_all_by_enrolled_at).alias("next_enrolled_at_2"),
    )
)

first_rows = (
    ranked
    .filter((F.col("score_rank") <= 5) & (F.to_date(F.col("next_enrolled_at")) < F.lit("2025-02-12").cast("date")))
    .drop("score_rank")
)

ranked_2 = (
    first_rows
    .select(
        "id",
        "term",
        "term_course_id_min_score",
        "score_vs_student_id_term_avg",
        "score_share_of_term_course_id",
        "next_enrolled_at",
        "last_known_enrolled_at",
        "next3_enrolled_at",
        "next_enrolled_at_2",
        F.max("score_vs_student_id_term_avg").over(w_enrolled_at_month).alias("enrolled_at_month_max_score_vs_student_id_term_a"),
        F.lag("score", 1, 0).over(w_term_by_next_enrolled_at_2).alias("prev_score"),
        F.ntile(10).over(w_term_by_next_enrolled_at_2).alias("next_enrolled_at_2_decile"),
        F.first("enrolled_at", ignorenulls=True).over(w_term_enrolled_at_month_by_te).alias("first_known_enrolled_at"),
        F.sum("last_score").over(w_enrolled_at_month).alias("enrolled_at_month_sum_last_score"),
        F.round(F.cume_dist().over(w_term_enrolled_at_month_by_te_2), 3).alias("term_course_id_min_score_cume_dist"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "term",
        "term_course_id_min_score",
        "score_vs_student_id_term_avg",
        "last_known_enrolled_at",
        "next3_enrolled_at",
        "next_enrolled_at_2",
        "enrolled_at_month_max_score_vs_student_id_term_a",
        "prev_score",
        "first_known_enrolled_at",
        "term_course_id_min_score_cume_dist",
        (F.floor(F.col("prev_score") / 25) * 25).alias("prev_score_bucket"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
    .orderBy(
        F.col("prev_score_bucket").desc(),
        F.col("term_course_id_min_score_cume_dist").asc_nulls_last(),
    )
)
