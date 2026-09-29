from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_bucket_ties = Window.orderBy(F.col("score_bucket").desc())
w_all_by_score_bucket = Window.orderBy("score_bucket", "id")
w_all_by_score_bucket_ties_2 = Window.orderBy("score_bucket")
w_course_level = Window.partitionBy("course_level")
w_all_by_score_bucket_2 = Window.orderBy(F.col("score_bucket").desc(), "id")
w_all_by_score_bucket_full = Window.orderBy("score_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.when(F.col("score") >= 59.7, "large").when(F.col("score") >= 32.7, "medium").otherwise("small").alias("score_tier"),
    )
)

joined_course = (
    filtered.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.score_bucket",
        "s.score_tier",
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

ranked = (
    joined_course
    .select(
        "*",
        F.rank().over(w_all_by_score_bucket_ties).alias("score_bucket_rank"),
        (F.col("score_bucket") - F.lag("score_bucket").over(w_all_by_score_bucket)).alias("score_bucket_change"),
        F.round(F.cume_dist().over(w_all_by_score_bucket_ties_2), 3).alias("score_bucket_cume_dist"),
        F.count(F.lit(1)).over(w_course_level).alias("rows_in_course_level"),
        F.size(F.collect_set("grade_status").over(w_course_level)).alias("distinct_grade_status_in_course_level"),
        F.lag("score_bucket").over(w_all_by_score_bucket_2).alias("prev_score_bucket"),
        F.lag("score_bucket", 3).over(w_all_by_score_bucket_2).alias("prev3_score_bucket"),
        F.round(F.col("score_bucket") - F.avg("score_bucket").over(w_course_level), 2).alias("score_bucket_vs_course_level_avg"),
        F.round(F.col("score_bucket") / F.nullif(F.sum("score_bucket").over(w_course_level), F.lit(0)), 4).alias("score_bucket_share_of_course_level"),
        F.rank().over(w_all_by_score_bucket_ties_2).alias("score_bucket_rank_2"),
        F.sum("score_bucket").over(w_course_level).alias("course_level_sum_score_bucket"),
        F.last("score_bucket").over(w_all_by_score_bucket_full).alias("last_score_bucket"),
    )
)

result = (
    ranked
    .filter(F.col("score_tier").isNotNull())
    .select(
        "id",
        "student_id",
        "grade_status",
        "score_bucket",
        "score_tier",
        "course_level",
        "score_bucket_cume_dist",
        "distinct_grade_status_in_course_level",
        "prev_score_bucket",
        "score_bucket_vs_course_level_avg",
        "score_bucket_rank_2",
        "course_level_sum_score_bucket",
        "last_score_bucket",
    )
    .orderBy("score_bucket_rank_2", F.col("grade_status").desc_nulls_first(), "id")
    .limit(5)
)
