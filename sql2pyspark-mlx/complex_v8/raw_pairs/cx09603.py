from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("course_id").orderBy(F.col("score").desc(), F.col("id").desc())
w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_course_id_by_enrolled_at_yea = Window.partitionBy("course_id").orderBy(F.col("enrolled_at_year").asc_nulls_last(), "id")
w_student_id_avg_score_tier_by = Window.partitionBy("student_id", "avg_score_tier").orderBy("avg_score_share_of_student_id_course_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_year_month = Window.partitionBy(F.date_trunc("month", "enrolled_at_year"))
w_course_id_by_enrolled_at_yea_2 = Window.partitionBy("course_id").orderBy(F.col("enrolled_at_year").asc_nulls_last())
w_student_id_avg_score_tier_by_2 = Window.partitionBy("student_id", "avg_score_tier").orderBy("avg_score_share_of_student_id_course_id", "id").rowsBetween(-1, Window.currentRow)

base_enrollments = (
    df_enrollments
    .filter((F.col("term") != "2024T2") & (F.col("grade_status") == "WITHDRAWN"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
        F.date_trunc("hour", F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

without_matches = (
    base_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id") <= 4), "left_anti")
)

course_id_stats = (
    without_matches
    .groupBy("course_id")
    .agg(F.avg("score").alias("avg_score"))
)

above_average = (
    without_matches.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

enriched = (
    above_average
    .filter(F.col("grade_status").isin("PASS", "WITHDRAWN", "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "grade_status_ref",
        "enrolled_at_hour",
        "enrolled_at_year",
        "avg_score",
        "score_minus_avg",
        F.when(F.col("avg_score") >= 65.0, "HIGH").when(F.col("avg_score") >= 25.0, "MEDIUM").otherwise("LOW").alias("avg_score_tier"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    latest
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status_ref",
        "enrolled_at_year",
        "score_minus_avg",
        "avg_score_tier",
        F.min("avg_score").over(w_student_id_course_id).alias("student_id_course_id_min_avg_score"),
        F.count(F.lit(1)).over(w_student_id_course_id).alias("rows_in_student_id_course_id"),
        F.round(F.col("avg_score") / F.nullif(F.sum("avg_score").over(w_student_id_course_id), F.lit(0)), 4).alias("avg_score_share_of_student_id_course_id"),
    )
)

with_trends = (
    ranked
    .select(
        "*",
        F.round(F.cume_dist().over(w_course_id_by_enrolled_at_yea), 3).alias("enrolled_at_year_cume_dist"),
        F.nth_value("rows_in_student_id_course_id", 2).over(w_student_id_avg_score_tier_by).alias("second_rows_in_student_id_course_id"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_year_month), F.lit(0)), 4).alias("score_share_of_enrolled_at_year_month"),
        F.rank().over(w_course_id_by_enrolled_at_yea_2).alias("enrolled_at_year_rank"),
        F.sum("rows_in_student_id_course_id").over(w_student_id_avg_score_tier_by_2).alias("rows_in_student_id_course_id_sum_2"),
        F.round(F.col("rows_in_student_id_course_id") / F.nullif(F.sum("rows_in_student_id_course_id").over(w_enrolled_at_year_month), F.lit(0)), 4).alias("rows_in_student_id_course_id_share_of_enrolled_a"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "avg_score_tier",
        "student_id_course_id_min_avg_score",
        "rows_in_student_id_course_id",
        "avg_score_share_of_student_id_course_id",
        "enrolled_at_year_cume_dist",
        "second_rows_in_student_id_course_id",
        "enrolled_at_year_rank",
        "rows_in_student_id_course_id_sum_2",
        "rows_in_student_id_course_id_share_of_enrolled_a",
        F.coalesce(F.col("avg_score_tier"), F.lit("UNKNOWN")).alias("avg_score_tier_or_unknown"),
    )
    .orderBy("course_id")
)
