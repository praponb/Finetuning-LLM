from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_or_unknown_by_s = Window.partitionBy("grade_status_or_unknown").orderBy("score", "id")
w_student_id_term = Window.partitionBy("student_id", "term")
w_grade_status_or_unknown_by_s_2 = Window.partitionBy("grade_status_or_unknown").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month_student_id = Window.partitionBy(F.date_trunc("month", "enrolled_at"), "student_id").orderBy("prev2_score", "id")
w_enrolled_at_month_student_id_2 = Window.partitionBy(F.date_trunc("month", "enrolled_at"), "student_id").orderBy("prev2_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_to_date = Window.partitionBy(F.to_date("enrolled_at"))

recent_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-19 15:00:00").cast("timestamp")) | (~F.col("term").isin("2025T2", "2024T1")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
)

windowed = (
    recent_enrollments
    .select(
        "*",
        F.lag("score", 2).over(w_grade_status_or_unknown_by_s).alias("prev2_score"),
        F.max("score").over(w_student_id_term).alias("student_id_term_max_score"),
        F.nth_value("enrolled_at", 2).over(w_grade_status_or_unknown_by_s_2).alias("second_enrolled_at"),
        F.min("score").over(w_student_id_term).alias("student_id_term_min_score"),
        F.round(F.cume_dist().over(w_grade_status_or_unknown_by_s), 3).alias("score_cume_dist"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.ntile(5).over(w_enrolled_at_month_student_id).alias("prev2_score_quintile"),
        F.ntile(4).over(w_enrolled_at_month_student_id).alias("prev2_score_quartile"),
        F.last("enrolled_at", ignorenulls=True).over(w_enrolled_at_month_student_id_2).alias("last_known_enrolled_at"),
        F.lag("enrolled_at").over(w_enrolled_at_month_student_id).alias("prev_enrolled_at"),
        F.size(F.collect_set("term").over(w_enrolled_at_to_date)).alias("distinct_term_in_enrolled_at_to_date"),
    )
)

labeled = (
    windowed_2
    .select(
        "*",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.upper(F.trim(F.col("enrolled_at_label"))).alias("enrolled_at_label_clean"),
    )
)

result = (
    labeled
    .groupingSets(
        [["distinct_term_in_enrolled_at_to_date", "course_id"], ["distinct_term_in_enrolled_at_to_date"], []],
        "distinct_term_in_enrolled_at_to_date", "course_id",
    )
    .agg(
        F.grouping("distinct_term_in_enrolled_at_to_date").alias("is_total_distinct_term_in_enrolled_at_to_date"),
        F.round(F.stddev("score_cume_dist"), 2).alias("stddev_score_cume_dist"),
    )
    .orderBy(
        F.col("distinct_term_in_enrolled_at_to_date").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
