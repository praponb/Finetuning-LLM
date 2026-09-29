from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits_filled_distance = Window.partitionBy("credits_filled_distance")
w_enrollment_enrolled_at_month = Window.partitionBy(F.date_trunc("month", "enrollment_enrolled_at"), "credits_filled").orderBy("credits", "id", "enrollment_id")
w_enrollment_enrolled_at_month_2 = Window.partitionBy(F.date_trunc("month", "enrollment_enrolled_at"), "credits_filled").orderBy("credits")

prepared = (
    df_courses
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        F.coalesce(F.col("dept"), F.lit("UNKNOWN")).alias("dept_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
    )
)

annotated = (
    prepared
    .filter(F.col("course_code").isNotNull())
    .select(
        "*",
        F.concat_ws(" / ", F.col("dept_slug"), F.col("course_code")).alias("dept_slug_course_code_label"),
        F.abs(F.col("credits_filled") - 2).alias("credits_filled_distance"),
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
    )
)

exploded_detail = (
    annotated.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.score") >= 20.6), "inner")
    .select(
        "s.id",
        "s.credits",
        "s.level",
        "s.dept_or_unknown",
        "s.credits_filled",
        "s.credits_filled_distance",
        "s.level_or_unknown",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

sequenced = (
    exploded_detail
    .select(
        "id",
        "credits",
        "level",
        "dept_or_unknown",
        "credits_filled",
        "level_or_unknown",
        "enrollment_id",
        "enrollment_score",
        "enrollment_student_id",
        "enrollment_term",
        F.size(F.collect_set("level_or_unknown").over(w_credits_filled_distance)).alias("distinct_level_or_unknown_in_credits_filled_dist"),
        F.round(F.percent_rank().over(w_enrollment_enrolled_at_month), 3).alias("credits_percent_rank"),
        F.dense_rank().over(w_enrollment_enrolled_at_month_2).alias("credits_dense_rank"),
        F.max("enrollment_score").over(w_credits_filled_distance).alias("credits_filled_distance_max_enrollment_score"),
    )
)

level_stats = (
    sequenced
    .groupBy("level")
    .agg(F.max("credits_dense_rank").alias("max_credits_dense_rank"))
)

compared = (
    sequenced.alias("s")
    .join(level_stats.alias("g"), F.col("g.level") == F.col("s.level"), "inner")
    .filter(F.col("s.credits_dense_rank") == F.col("g.max_credits_dense_rank"))
    .select(
        "s.*",
        "g.max_credits_dense_rank",
        F.round(F.col("s.credits_dense_rank") - F.col("g.max_credits_dense_rank"), 2).alias("credits_dense_rank_minus_max"),
    )
)

result = (
    compared
    .select(
        "id",
        "credits",
        "level",
        "dept_or_unknown",
        "credits_filled",
        "level_or_unknown",
        "enrollment_id",
        "enrollment_student_id",
        "enrollment_term",
        "distinct_level_or_unknown_in_credits_filled_dist",
        "credits_percent_rank",
        "credits_dense_rank",
        "credits_filled_distance_max_enrollment_score",
    )
    .orderBy(F.col("level").asc_nulls_last())
)
