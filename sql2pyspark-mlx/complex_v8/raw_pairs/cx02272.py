from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_course_id_student_id = Window.partitionBy("course_id", "student_id")
w_student_id_term_by_enrolled = Window.partitionBy("student_id", "term").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at")

prepared = (
    df_enrollments
    .filter((~F.col("term").isin("2024T2", "2025T1")) & (F.to_date(F.col("enrolled_at")) < F.lit("2025-02-12").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.min("score").over(w_grade_status_course_id).alias("grade_status_course_id_min_score"),
        F.size(F.collect_set("term").over(w_course_id_student_id)).alias("distinct_term_in_course_id_student_id"),
        F.round(F.avg("score").over(w_grade_status_course_id), 2).alias("grade_status_course_id_avg_score"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_id_term_by_enrolled))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.ntile(4).over(w_all_by_enrolled_at).alias("enrolled_at_quartile"),
        F.max("score").over(w_course_id_student_id).alias("course_id_student_id_max_score"),
        F.ntile(4).over(w_grade_status_by_score).alias("score_quartile"),
        F.round(F.percent_rank().over(w_grade_status_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
    )
)

labeled = (
    windowed
    .withColumn("enrolled_at_quartile_per_score_quartile", F.round(F.col("enrolled_at_quartile") / F.nullif(F.col("score_quartile"), F.lit(0)), 3))
)

totals = (
    labeled
    .groupBy(
        F.year("enrolled_at").alias("enrolled_at_year"),
        "enrolled_at_quartile",
        "course_id",
    )
    .agg(
        F.countDistinct("term").alias("distinct_terms"),
        F.percentile_approx("distinct_term_in_course_id_student_id", 0.5).alias("median_distinct_term_in_course_id_student_id"),
        F.count(F.when(F.col("term") == "2024T1", 1)).alias("n_term_2024t1"),
        F.max("score_quartile").alias("max_score_quartile"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
    )
)

result = (
    totals
    .select(
        "enrolled_at_year",
        "enrolled_at_quartile",
        "course_id",
        "distinct_terms",
        "median_distinct_term_in_course_id_student_id",
        "n_term_2024t1",
        "max_score_quartile",
        F.abs(F.col("distinct_terms") - 6).alias("distinct_terms_distance"),
    )
    .orderBy(F.col("enrolled_at_year").desc(), "enrolled_at_quartile", "course_id")
    .limit(20)
)
