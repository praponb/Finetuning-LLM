from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_score_tier_student_is_intern = Window.partitionBy("score_tier", "student_is_international").orderBy("enrolled_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_major_enrolled_at_da = Window.partitionBy("student_major", F.year("enrolled_at_date"))
w_student_id_score_tier_by_day = Window.partitionBy("student_id", "score_tier").orderBy(F.col("days_since_enrolled_at_2").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_days_since_enrolled_a = Window.orderBy("days_since_enrolled_at", "id")
w_student_id_score_tier_by_day_2 = Window.partitionBy("student_id", "score_tier").orderBy(F.col("days_since_enrolled_at_2").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)

scoped = (
    df_enrollments
    .filter(F.month(F.col("enrolled_at")).isin(1, 3))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.when(F.col("score") >= 77.0, "large").when(F.col("score") >= 20.2, "medium").otherwise("small").alias("score_tier"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score").between(23.7, 85.8)), "left_anti")
)

annotated = (
    without_matches
    .withColumn("days_since_enrolled_at_2", F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")))
)

labeled = (
    annotated
    .filter(F.col("score") <= 57.8)
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "days_since_enrolled_at",
        "score_tier",
        "days_since_enrolled_at_2",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

combined_details = (
    labeled.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.major") == "Biz"), "left")
    .select(
        "s.*",
        F.col("st.major").alias("student_major"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.min("score").alias("min_enrollment_score"))
)

summarized_rows = (
    combined_details.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.min_enrollment_score")
)

windowed = (
    summarized_rows
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "score_tier",
        "days_since_enrolled_at_2",
        "enrolled_at_date",
        "min_enrollment_score",
        F.first("score", ignorenulls=True).over(w_score_tier_student_is_intern).alias("first_known_score"),
        F.size(F.collect_set("score_tier").over(w_student_major_enrolled_at_da)).alias("distinct_score_tier_in_student_major_enrolled_at"),
        F.first("min_enrollment_score").over(w_student_id_score_tier_by_day).alias("first_min_enrollment_score"),
        F.lead("score", 2).over(w_all_by_days_since_enrolled_a).alias("next2_score"),
        F.sum("days_since_enrolled_at").over(w_student_id_score_tier_by_day_2).alias("days_since_enrolled_at_sum_2"),
        F.size(F.collect_set("student_id").over(w_student_major_enrolled_at_da)).alias("distinct_student_id_in_student_major_enrolled_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "score",
        "score_tier",
        "days_since_enrolled_at_2",
        "enrolled_at_date",
        "min_enrollment_score",
        "first_known_score",
        "distinct_score_tier_in_student_major_enrolled_at",
        "first_min_enrollment_score",
        "next2_score",
        "days_since_enrolled_at_sum_2",
        "distinct_student_id_in_student_major_enrolled_at",
    )
    .orderBy(F.col("first_known_score").asc(), F.col("min_enrollment_score").desc())
)
