from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc(), "id")
w_term_by_enrolled_at_full = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status = Window.partitionBy("grade_status")
w_term_grade_status_by_score_t = Window.partitionBy("term", "grade_status").orderBy("score")
w_student_id_term_by_score_tie = Window.partitionBy("student_id", "term").orderBy("score")
w_student_id_term_by_enrolled = Window.partitionBy("student_id", "term").orderBy(F.col("enrolled_at").desc())
w_course_id = Window.partitionBy("course_id")

prepared = (
    df_enrollments
    .filter(F.col("score") < 57.6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        (F.col("score") - F.lag("score").over(w_course_id_by_enrolled_at)).alias("score_change"),
        F.lag("enrolled_at").over(w_course_id_by_enrolled_at).alias("prev_enrolled_at"),
        F.nth_value("enrolled_at", 2).over(w_term_by_enrolled_at_full).alias("second_enrolled_at"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status), 2).alias("score_vs_grade_status_avg"),
        F.rank().over(w_term_grade_status_by_score_t).alias("score_rank"),
        F.size(F.collect_set("term").over(w_grade_status)).alias("distinct_term_in_grade_status"),
        F.rank().over(w_student_id_term_by_score_tie).alias("score_rank_2"),
        F.rank().over(w_student_id_term_by_enrolled).alias("enrolled_at_rank"),
        F.rank().over(w_course_id_by_enrolled_at).alias("enrolled_at_rank_2"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "score_change",
        "prev_enrolled_at",
        "second_enrolled_at",
        "score_vs_grade_status_avg",
        "score_rank",
        "score_rank_2",
        "enrolled_at_rank",
        "enrolled_at_rank_2",
        "rows_in_course_id",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

result = (
    scored
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "score_change",
        "prev_enrolled_at",
        "second_enrolled_at",
        "score_rank",
        "score_rank_2",
        "enrolled_at_rank",
        "enrolled_at_rank_2",
        "grade_status_slug",
        F.round(F.col("score_rank") / F.nullif(F.col("enrolled_at_rank_2"), F.lit(0)), 3).alias("score_rank_per_enrolled_at_rank_2"),
        (F.col("prev_enrolled_at") + F.expr("INTERVAL 15 MINUTES")).alias("prev_enrolled_at_plus_15m"),
    )
)
