from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy("score", "id")
w_student_id_by_score = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id")
w_student_major_student_is_int = Window.partitionBy("student_major", "student_is_international")
w_student_id_student_enrolled = Window.partitionBy("student_id", F.year("student_enrolled_on"))
w_student_is_international_by = Window.partitionBy("student_is_international").orderBy("enrolled_at", "id")
w_student_enrolled_on_month_by = Window.partitionBy(F.month("student_enrolled_on")).orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_student_is_international_by_2 = Window.partitionBy("student_is_international").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_tercile = Window.partitionBy("enrolled_at_tercile")
w_all_by_prev2_score_moving_2 = Window.orderBy("prev2_score", "id").rowsBetween(-2, Window.currentRow)
w_prev3_student_enrolled_on_mo = Window.partitionBy(F.date_trunc("month", "prev3_student_enrolled_on")).orderBy(F.col("rows_in_student_id_student_enrolled_on_year").desc_nulls_first(), "id")
w_all_by_prev2_score_ties = Window.orderBy("prev2_score")

valid_enrollments = (
    df_enrollments
    .filter(F.col("course_id") >= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

combined_details = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        F.col("st.major").alias("student_major"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

with_trends = (
    combined_details
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "student_major",
        "student_is_international",
        F.dense_rank().over(w_all_by_score).alias("score_dense_rank"),
        F.lag("student_enrolled_on", 3).over(w_student_id_by_score).alias("prev3_student_enrolled_on"),
        F.sum("score").over(w_student_major_student_is_int).alias("student_major_student_is_international_sum_score"),
        F.min("score").over(w_student_id_student_enrolled).alias("student_id_student_enrolled_on_year_min_score"),
        F.ntile(3).over(w_student_is_international_by).alias("enrolled_at_tercile"),
        F.lag("score", 2).over(w_student_enrolled_on_month_by).alias("prev2_score"),
        F.datediff(F.col("student_enrolled_on"), F.lag("student_enrolled_on").over(w_all_by_score)).alias("days_since_prev_student_enrolled_on"),
        F.size(F.collect_set("term").over(w_student_major_student_is_int)).alias("distinct_term_in_student_major_student_is_intern"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_score))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.first("score").over(w_student_is_international_by_2).alias("first_score"),
        F.round(F.avg("score").over(w_student_major_student_is_int), 2).alias("student_major_student_is_international_avg_score"),
        F.count(F.lit(1)).over(w_student_id_student_enrolled).alias("rows_in_student_id_student_enrolled_on_year"),
        F.nth_value("score", 2).over(w_student_is_international_by_2).alias("second_score"),
        F.sum("score").over(w_student_id_student_enrolled).alias("student_id_student_enrolled_on_year_sum_score"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.sum("score_dense_rank").over(w_enrolled_at_tercile).alias("enrolled_at_tercile_sum_score_dense_rank"),
        F.sum("enrolled_at_tercile").over(w_all_by_prev2_score_moving_2).alias("enrolled_at_tercile_sum_3"),
        F.count(F.lit(1)).over(w_enrolled_at_tercile).alias("rows_in_enrolled_at_tercile"),
        F.lead("prev3_student_enrolled_on", 2).over(w_prev3_student_enrolled_on_mo).alias("next2_prev3_student_enrolled_on"),
        F.min("score_dense_rank").over(w_enrolled_at_tercile).alias("enrolled_at_tercile_min_score_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_prev2_score_ties), 3).alias("prev2_score_cume_dist"),
        F.round(F.col("enrolled_at_tercile") - F.avg("enrolled_at_tercile").over(w_enrolled_at_tercile), 2).alias("enrolled_at_tercile_vs_enrolled_at_tercile_avg"),
    )
)

grouped = (
    metrics
    .filter(F.col("student_id_student_enrolled_on_year_min_score").between(200.0, 600.0))
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("student_is_international"), 1)).alias("n_student_is_international_tru"),
        F.count("*").alias("n_rows"),
        F.round(F.stddev("prev2_score"), 2).alias("stddev_prev2_score"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.max("next2_prev3_student_enrolled_on").alias("last_next2_prev3_student_enrolled_on"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    grouped
    .select(
        "course_id",
        "n_student_is_international_tru",
        "n_rows",
        "stddev_prev2_score",
        "last_next2_prev3_student_enrolled_on",
    )
    .orderBy("n_rows")
)
