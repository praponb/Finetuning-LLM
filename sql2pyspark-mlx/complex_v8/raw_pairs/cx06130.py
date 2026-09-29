from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_score_tier_by_enrolled_at = Window.partitionBy("score_tier").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_score_filled_ties = Window.orderBy(F.col("score_filled").desc())
w_grade_status = Window.partitionBy("grade_status")
w_dedup = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")

recent = (
    df_enrollments
    .filter((F.col("course_id") >= 3) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.when(F.col("score") >= 63.9, "A").when(F.col("score") >= 34.2, "B").otherwise("C").alias("score_tier"),
    )
)

sequenced = (
    recent
    .select(
        "*",
        F.lead("score", 2).over(w_score_tier_by_enrolled_at).alias("next2_score"),
        F.dense_rank().over(w_all_by_score_filled_ties).alias("score_filled_dense_rank"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
    )
)

scored = (
    sequenced
    .filter(F.col("grade_status").isin("WITHDRAWN", "FAIL"))
    .select(
        "*",
        (F.col("rows_in_grade_status") + F.col("next2_score")).alias("rows_in_grade_status_plus_next2_score"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        (F.floor(F.col("score_filled_dense_rank") / 2) * 2).alias("score_filled_dense_rank_bucket"),
    )
)

with_activity = (
    scored.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id").isNotNull()), "left_anti")
    .filter(F.col("enrolled_at") < F.lit("2025-02-24 14:00:00").cast("timestamp"))
)

first_rows = (
    with_activity
    .filter(F.col("score_filled_dense_rank") <= 5)
    .drop("score_filled_dense_rank")
)

joined_course = (
    first_rows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_filled",
        "s.next2_score",
        "s.days_since_enrolled_at",
        "s.score_filled_dense_rank_bucket",
        F.col("c.level").alias("course_level"),
    )
)

latest_per_key = (
    joined_course
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .filter(F.col("days_since_enrolled_at") < 560)
    .select(
        "id",
        "student_id",
        "course_id",
        "score_filled",
        "next2_score",
        "days_since_enrolled_at",
        "score_filled_dense_rank_bucket",
        "course_level",
        F.lower(F.trim(F.col("course_level"))).alias("course_level_clean"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
    .orderBy(F.col("course_level_clean").asc())
)
