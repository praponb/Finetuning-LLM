from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_score_full = Window.partitionBy("grade_status").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(-6, -1)
w_grade_status_by_score_full_2 = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_student_id = Window.partitionBy("term", "student_id")
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at_quarter").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_quarter_month = Window.partitionBy(F.date_trunc("month", "enrolled_at_quarter"))
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_grade_status_by_enrolled_at_3 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at_quarter").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_grade_status_by_sc = Window.partitionBy("course_id", "grade_status").orderBy(F.col("score_vs_student_id_grade_status_avg").desc())
w_course_id_grade_status_by_sc_2 = Window.partitionBy("course_id", "grade_status").orderBy(F.col("score_vs_student_id_grade_status_avg").desc(), "id")
w_course_id_grade_status_by_sc_3 = Window.partitionBy("course_id", "grade_status").orderBy(F.col("score_vs_student_id_grade_status_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id = Window.partitionBy("student_id")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-24 08:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

ranked = (
    scoped_enrollments
    .select(
        "*",
        F.nth_value("score", 2).over(w_grade_status_by_score_full).alias("second_score"),
        F.max("score").over(w_grade_status_by_enrolled_at).alias("score_max_6_prior"),
        F.nth_value("enrolled_at_quarter", 2).over(w_grade_status_by_score_full_2).alias("second_enrolled_at_quarter"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term_student_id), F.lit(0)), 4).alias("score_share_of_term_student_id"),
        F.lag("score", 2, 0).over(w_grade_status_by_score).alias("prev2_score"),
        F.first("enrolled_at_quarter").over(w_grade_status_by_enrolled_at_2).alias("first_enrolled_at_quarter"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_quarter_month), F.lit(0)), 4).alias("score_share_of_enrolled_at_quarter_month"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_grade_status), F.lit(0)), 4).alias("score_share_of_student_id_grade_status"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_quarter_month)).alias("distinct_course_id_in_enrolled_at_quarter_month"),
        F.round(F.col("score") - F.avg("score").over(w_student_id_grade_status), 2).alias("score_vs_student_id_grade_status_avg"),
        F.max("score").over(w_grade_status_by_enrolled_at_3).alias("max_score_so_far"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.rank().over(w_course_id_grade_status_by_sc).alias("score_vs_student_id_grade_status_avg_rank"),
        F.ntile(3).over(w_course_id_grade_status_by_sc_2).alias("score_vs_student_id_grade_status_avg_tercile"),
        F.first("enrolled_at").over(w_course_id_grade_status_by_sc_3).alias("first_enrolled_at"),
        F.max("score_max_6_prior").over(w_student_id).alias("student_id_max_score_max_6_prior"),
    )
)

result = (
    with_windows
    .filter(F.col("score_vs_student_id_grade_status_avg_tercile") < 9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at_quarter",
        "second_score",
        "score_max_6_prior",
        "second_enrolled_at_quarter",
        "score_share_of_term_student_id",
        "prev2_score",
        "score_share_of_student_id_grade_status",
        "distinct_course_id_in_enrolled_at_quarter_month",
        "score_vs_student_id_grade_status_avg",
        "max_score_so_far",
        "score_vs_student_id_grade_status_avg_rank",
        "score_vs_student_id_grade_status_avg_tercile",
        "student_id_max_score_max_6_prior",
        F.date_trunc("hour", F.col("enrolled_at_quarter")).alias("enrolled_at_quarter_hour"),
        F.when(F.col("grade_status").isin("FAIL", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "PASS", "group_b").otherwise("other").alias("grade_status_group"),
    )
)
