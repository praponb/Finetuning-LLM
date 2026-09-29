from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_score_full = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_grade_status = Window.partitionBy("course_id", "grade_status")
w_grade_status_student_id_by_s = Window.partitionBy("grade_status", "student_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")
w_course_id = Window.partitionBy("course_id")
w_grade_status_course_id_by_en = Window.partitionBy("grade_status", "course_id").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")

prepared = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

sequenced = (
    prepared
    .select(
        "*",
        F.nth_value("score", 2).over(w_grade_status_by_score_full).alias("second_score"),
        F.count(F.lit(1)).over(w_course_id_grade_status).alias("rows_in_course_id_grade_status"),
        F.first("score").over(w_grade_status_student_id_by_s).alias("first_score"),
        F.round(F.col("score") - F.avg("score").over(w_course_id_grade_status), 2).alias("score_vs_course_id_grade_status_avg"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_grade_status_by_score))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.rank().over(w_grade_status_course_id_by_en).alias("enrolled_at_rank"),
        F.min("score").over(w_course_id_grade_status).alias("course_id_grade_status_min_score"),
    )
)

summary = (
    sequenced
    .filter(F.col("student_id").isNotNull())
    .groupBy("course_id", "enrolled_at_rank", "student_id")
    .agg(
        F.sum("course_id_grade_status_min_score").alias("total_course_id_grade_status_min_score"),
        F.percentile_approx("minutes_since_prev_enrolled_at", 0.5).alias("median_minutes_since_prev_enrolled_at"),
        F.count(F.when(~F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN"), 1)).alias("n_grade_status_not_in_incomple"),
        F.count("*").alias("n_rows"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.round(F.avg("second_score"), 2).alias("avg_second_score"),
    )
)

result = (
    summary
    .filter(F.col("n_rows") < 8)
    .select(
        "course_id",
        "enrolled_at_rank",
        "student_id",
        "median_minutes_since_prev_enrolled_at",
        "n_rows",
        "distinct_course_ids",
        "avg_second_score",
    )
    .orderBy(F.col("median_minutes_since_prev_enrolled_at").desc())
)
