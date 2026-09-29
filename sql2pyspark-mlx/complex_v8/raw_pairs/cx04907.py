from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_student_id = Window.partitionBy("student_id")
w_all_by_score_moving_2 = Window.orderBy("score", "id").rowsBetween(-2, Window.currentRow)
w_course_id_student_id_by_scor = Window.partitionBy("course_id", "student_id").orderBy(F.col("score").desc_nulls_first(), "id")
w_course_id_student_id_by_scor_2 = Window.partitionBy("course_id", "student_id").orderBy(F.col("score").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id = Window.partitionBy("course_id")
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_all_by_score = Window.orderBy("score", "id")
w_course_id_student_id_by_scor_3 = Window.partitionBy("course_id", "student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_student_id_by_scor_4 = Window.partitionBy("course_id", "student_id").orderBy(F.col("score").desc_nulls_first(), "id").rowsBetween(-2, -1)

scoped_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2025T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_details = (
    scoped_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "c.course_code",
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_student_id_course_id).alias("rows_in_student_id_course_id"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
        F.round(F.avg("score").over(w_all_by_score_moving_2), 2).alias("score_avg_3"),
        F.dense_rank().over(w_course_id_student_id_by_scor).alias("score_dense_rank"),
        F.size(F.collect_set("course_id").over(w_student_id)).alias("distinct_course_id_in_student_id"),
        F.nth_value("score", 2).over(w_course_id_student_id_by_scor_2).alias("second_score"),
        F.nth_value("enrolled_at", 2).over(w_course_id_student_id_by_scor_2).alias("second_enrolled_at"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.lead("enrolled_at").over(w_student_id_by_enrolled_at).alias("next_enrolled_at"),
        F.dense_rank().over(w_all_by_score).alias("score_dense_rank_2"),
        F.first("score").over(w_course_id_student_id_by_scor_3).alias("first_score"),
        F.sum("score").over(w_course_id_student_id_by_scor_4).alias("score_sum_2_prior"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_id_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        (F.col("score") - F.lag("score").over(w_student_id_by_enrolled_at)).alias("score_change"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "rows_in_student_id_course_id",
        "score_vs_student_id_avg",
        "second_enrolled_at",
        "rows_in_course_id",
        "next_enrolled_at",
        "first_score",
        "score_sum_2_prior",
        "minutes_since_prev_enrolled_at",
        F.date_trunc("year", F.col("second_enrolled_at")).alias("second_enrolled_at_year"),
        F.abs(F.col("first_score") - 63.9).alias("first_score_distance"),
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
    )
)

totals = (
    enriched
    .groupBy(
        "enrolled_at_dayofweek",
        F.when(F.col("first_score_distance") >= 65.0, "large").when(F.col("first_score_distance") >= 40.0, "medium").otherwise("small").alias("first_score_distance_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("enrolled_at_dayofweek").alias("total_enrolled_at_dayofweek"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.max("enrolled_at_dayofweek").alias("max_enrolled_at_dayofweek"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .filter(F.col("first_score_distance_tier") == "large")
    .select(
        "enrolled_at_dayofweek",
        "first_score_distance_tier",
        "distinct_ids",
        "total_enrolled_at_dayofweek",
        "distinct_student_ids",
        "max_enrolled_at_dayofweek",
        "first_enrolled_at",
        F.hour(F.col("first_enrolled_at")).alias("first_enrolled_at_hour"),
        F.greatest(F.col("enrolled_at_dayofweek"), F.col("distinct_student_ids")).alias("greatest_enrolled_at_dayofweek_distinct_student_"),
    )
    .orderBy(
        F.col("distinct_student_ids").asc(),
        F.col("enrolled_at_dayofweek").desc(),
    )
)
