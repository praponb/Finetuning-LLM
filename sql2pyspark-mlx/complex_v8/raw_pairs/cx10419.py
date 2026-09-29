from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_enrolled_at = Window.partitionBy("term").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_term_by_enrolled_at_full = Window.partitionBy("term").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_student_id = Window.partitionBy("student_id")
w_all_by_score = Window.orderBy("score", "id")
w_all_by_score_prior_2 = Window.orderBy("score", "id").rowsBetween(-2, -1)
w_term = Window.partitionBy("term")
w_term_student_id_by_prev_scor = Window.partitionBy("term", "student_id").orderBy(F.col("prev_score").desc(), "id")
w_term_student_id_by_prev_scor_2 = Window.partitionBy("term", "student_id").orderBy(F.col("prev_score").desc())
w_term_by_score = Window.partitionBy("term").orderBy("score", "id")
w_term_student_id_by_prev_scor_3 = Window.partitionBy("term", "student_id").orderBy(F.col("prev_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_enrollments = (
    df_enrollments
    .filter((F.col("score") < 57.3) & (F.col("grade_status").isin("FAIL", "WITHDRAWN")))
)

with_trends = (
    scoped_enrollments
    .select(
        "*",
        F.round(F.percent_rank().over(w_term_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
        F.nth_value("score", 2).over(w_term_by_enrolled_at_full).alias("second_score"),
        (F.col("score") - F.lag("score").over(w_all_by_enrolled_at)).alias("score_change"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.lag("score").over(w_all_by_score).alias("prev_score"),
        F.round(F.avg("score").over(w_all_by_score_prior_2), 2).alias("score_avg_2_prior"),
        F.dense_rank().over(w_all_by_enrolled_at).alias("enrolled_at_dense_rank"),
        F.round(F.cume_dist().over(w_term_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "score_share_of_student_id",
        "prev_score",
        "score_avg_2_prior",
        "enrolled_at_dense_rank",
        "enrolled_at_cume_dist",
        F.max("second_score").over(w_term).alias("term_max_second_score"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_term_student_id_by_prev_scor))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.percent_rank().over(w_term_student_id_by_prev_scor_2), 3).alias("prev_score_percent_rank"),
        F.round(F.cume_dist().over(w_term_by_score), 3).alias("score_cume_dist"),
        F.count(F.lit(1)).over(w_term_student_id_by_prev_scor_3).alias("running_count"),
    )
)

result = (
    sequenced
    .filter(F.col("minutes_since_prev_enrolled_at") > 2250.0)
    .select(
        "id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "score_avg_2_prior",
        "enrolled_at_cume_dist",
        "minutes_since_prev_enrolled_at",
        "prev_score_percent_rank",
        "score_cume_dist",
        "running_count",
    )
    .orderBy(F.col("enrolled_at").desc(), F.col("term").desc(), "id")
    .limit(10)
)
