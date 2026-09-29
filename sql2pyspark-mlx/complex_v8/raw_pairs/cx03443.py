from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_enrolled_at_date = Window.partitionBy("student_id", F.month("enrolled_at_date"))
w_term_by_enrolled_at_date = Window.partitionBy("term").orderBy("enrolled_at_date", "id")
w_term_by_enrolled_at_date_ful = Window.partitionBy("term").orderBy("enrolled_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_distinct_course = Window.partitionBy("grade_status", "distinct_course_level_in_student_id_enrolled_at_")
w_all_by_prev3_score_bucket = Window.orderBy(F.col("prev3_score_bucket").desc(), "id")
w_course_level_by_prev3_score = Window.partitionBy("course_level").orderBy(F.col("prev3_score_bucket_minus_avg").desc(), "id")
w_distinct_course_level_in_stu = Window.partitionBy("distinct_course_level_in_student_id_enrolled_at_").orderBy("score_bucket_to_prev3_score_bucket_ratio", "distinct_course_level_in_student_id_enrolled_at_").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_total_prev3_score_buc = Window.orderBy("total_prev3_score_bucket_dense_rank", "distinct_course_level_in_student_id_enrolled_at_").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_enrollments
    .filter((F.col("score") <= 59.7) & (F.col("term").isin("2025T2", "2024T1")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

with_parent_course = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.score_bucket",
        "s.enrolled_at_date",
        F.col("c.level").alias("course_level"),
    )
)

sequenced = (
    with_parent_course
    .select(
        "id",
        "term",
        "grade_status",
        "score_bucket",
        "enrolled_at_date",
        "course_level",
        F.size(F.collect_set("course_level").over(w_student_id_enrolled_at_date)).alias("distinct_course_level_in_student_id_enrolled_at_"),
        F.lag("score_bucket", 3).over(w_term_by_enrolled_at_date).alias("prev3_score_bucket"),
        F.first("enrolled_at_date").over(w_term_by_enrolled_at_date_ful).alias("first_enrolled_at_date"),
    )
)

grade_status_stats = (
    sequenced
    .groupBy("grade_status")
    .agg(F.avg("prev3_score_bucket").alias("avg_prev3_score_bucket"))
)

compared = (
    sequenced.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.prev3_score_bucket") > F.col("g.avg_prev3_score_bucket"))
    .select(
        "s.*",
        "g.avg_prev3_score_bucket",
        F.round(F.col("s.prev3_score_bucket") - F.col("g.avg_prev3_score_bucket"), 2).alias("prev3_score_bucket_minus_avg"),
    )
)

metrics = (
    compared
    .select(
        "*",
        F.round(F.avg("prev3_score_bucket_minus_avg").over(w_grade_status_distinct_course), 2).alias("grade_status_distinct_course_level_in_student_id"),
        F.dense_rank().over(w_all_by_prev3_score_bucket).alias("prev3_score_bucket_dense_rank"),
        (F.col("prev3_score_bucket_minus_avg") - F.lag("prev3_score_bucket_minus_avg").over(w_course_level_by_prev3_score)).alias("prev3_score_bucket_minus_avg_change"),
        F.rank().over(w_course_level_by_prev3_score).alias("prev3_score_bucket_minus_avg_rank"),
        F.datediff(F.col("enrolled_at_date"), F.lag("enrolled_at_date").over(w_course_level_by_prev3_score)).alias("days_since_prev_enrolled_at_date"),
    )
)

rollup_base = (
    metrics
    .filter(F.col("grade_status").isNotNull())
    .groupBy("distinct_course_level_in_student_id_enrolled_at_")
    .agg(
        F.round(F.sum("score_bucket") / F.nullif(F.sum("distinct_course_level_in_student_id_enrolled_at_"), F.lit(0)), 3).alias("score_bucket_to_distinct_course_level_in_student"),
        F.round(F.sum("score_bucket") / F.nullif(F.sum("prev3_score_bucket"), F.lit(0)), 3).alias("score_bucket_to_prev3_score_bucket_ratio"),
        F.sum("prev3_score_bucket_dense_rank").alias("total_prev3_score_bucket_dense_rank"),
        F.min("first_enrolled_at_date").alias("first_first_enrolled_at_date"),
        F.sum(F.when(F.col("grade_status") == "WITHDRAWN", F.col("days_since_prev_enrolled_at_date")).otherwise(0)).alias("days_since_prev_enrolled_at_date_grade_status_wi"),
    )
)

sequenced_2 = (
    rollup_base
    .withColumn("second_first_first_enrolled_at_date", F.nth_value("first_first_enrolled_at_date", 2).over(w_distinct_course_level_in_stu))
    .withColumn("last_known_first_first_enrolled_at_date", F.last("first_first_enrolled_at_date", ignorenulls=True).over(w_all_by_total_prev3_score_buc))
)

result = (
    sequenced_2
    .select(
        "distinct_course_level_in_student_id_enrolled_at_",
        "total_prev3_score_bucket_dense_rank",
        "first_first_enrolled_at_date",
        "days_since_prev_enrolled_at_date_grade_status_wi",
        "second_first_first_enrolled_at_date",
    )
    .orderBy(
        F.col("first_first_enrolled_at_date").desc(),
        "days_since_prev_enrolled_at_date_grade_status_wi",
        "distinct_course_level_in_student_id_enrolled_at_",
    )
    .limit(5)
)
