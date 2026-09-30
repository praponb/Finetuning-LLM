from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_score_tier_grade_status_len = Window.partitionBy("score_tier", "grade_status_len")
w_grade_status_score_tier_by_s = Window.partitionBy("grade_status", "score_tier").orderBy("score", "id")
w_course_id_grade_status_len_b = Window.partitionBy("course_id", "grade_status_len").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_grade_status_score_tier_by_s_2 = Window.partitionBy("grade_status", "score_tier").orderBy("score")
w_distinct_student_id_in_score = Window.partitionBy("distinct_student_id_in_score_tier_grade_status_l").orderBy(F.col("score_change").desc(), "id")
w_distinct_student_id_in_score_2 = Window.partitionBy("distinct_student_id_in_score_tier_grade_status_l").orderBy(F.col("score_change").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_rank = Window.orderBy("score_rank", "id")

scoped = (
    df_enrollments
    .filter((F.col("term") == "2024T1") & (F.to_date(F.col("enrolled_at")) < F.lit("2025-03-14").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 74.9, "large").when(F.col("score") >= 35.8, "medium").otherwise("small").alias("score_tier"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.size(F.collect_set("student_id").over(w_score_tier_grade_status_len)).alias("distinct_student_id_in_score_tier_grade_status_l"),
        F.lag("score", 1, 0).over(w_grade_status_score_tier_by_s).alias("prev_score"),
        F.round(F.cume_dist().over(w_course_id_grade_status_len_b), 3).alias("enrolled_at_cume_dist"),
        (F.col("grade_status_len") - F.lag("grade_status_len").over(w_course_id_grade_status_len_b)).alias("grade_status_len_change"),
        F.rank().over(w_grade_status_score_tier_by_s_2).alias("score_rank"),
        F.max("score").over(w_score_tier_grade_status_len).alias("score_tier_grade_status_len_max_score"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_id_grade_status_len_b))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.col("score") - F.avg("score").over(w_score_tier_grade_status_len), 2).alias("score_vs_score_tier_grade_status_len_avg"),
        (F.col("score") - F.lag("score").over(w_course_id_grade_status_len_b)).alias("score_change"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.concat_ws(" / ", F.col("grade_status"), F.col("score_tier")).alias("grade_status_score_tier_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        (F.col("score_rank") - F.lag("score_rank").over(w_distinct_student_id_in_score)).alias("score_rank_change"),
        F.first("score_rank", ignorenulls=True).over(w_distinct_student_id_in_score_2).alias("first_known_score_rank"),
        (F.col("enrolled_at_cume_dist") - F.lag("enrolled_at_cume_dist").over(w_all_by_score_rank)).alias("enrolled_at_cume_dist_change"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "score_tier",
        "distinct_student_id_in_score_tier_grade_status_l",
        "enrolled_at_cume_dist",
        "score_rank",
        "minutes_since_prev_enrolled_at",
        "score_change",
        "grade_status_score_tier_label",
        "days_since_enrolled_at",
        "score_rank_change",
        "first_known_score_rank",
        "enrolled_at_cume_dist_change",
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("first_known_score_rank").desc(),
        "id",
    )
    .limit(5)
)
