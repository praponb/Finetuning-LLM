from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrolled_at_dat = Window.partitionBy("course_id").orderBy("enrolled_at_date", "id").rowsBetween(-1, -1)
w_term_enrolled_at_date_month = Window.partitionBy("term", F.month("enrolled_at_date")).orderBy(F.col("score").desc_nulls_first())
w_term_enrolled_at_date_month_2 = Window.partitionBy("term", F.month("enrolled_at_date")).orderBy(F.col("score").desc_nulls_first(), "id")
w_course_id_enrolled_at_date_y = Window.partitionBy("course_id", F.year("enrolled_at_date"))
w_course_id_by_enrolled_at_dat_2 = Window.partitionBy("course_id").orderBy("enrolled_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("score") >= 32.7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
    )
)

with_windows = (
    cleaned_enrollments
    .select(
        "*",
        F.sum("score").over(w_course_id_by_enrolled_at_dat).alias("score_sum_1_prior"),
        F.dense_rank().over(w_term_enrolled_at_date_month).alias("score_dense_rank"),
        F.ntile(4).over(w_term_enrolled_at_date_month_2).alias("score_quartile"),
        F.count(F.lit(1)).over(w_course_id_enrolled_at_date_y).alias("rows_in_course_id_enrolled_at_date_year"),
        F.nth_value("enrolled_at", 2).over(w_course_id_by_enrolled_at_dat_2).alias("second_enrolled_at"),
        F.round(F.avg("score").over(w_course_id_enrolled_at_date_y), 2).alias("course_id_enrolled_at_date_year_avg_score"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["course_id", "student_id", "score_dense_rank"], ["course_id"]],
        "course_id", "student_id", "score_dense_rank",
    )
    .agg(
        F.grouping("course_id").alias("is_total_course_id"),
        F.round(F.sum("course_id_enrolled_at_date_year_avg_score") / F.nullif(F.sum("rows_in_course_id_enrolled_at_date_year"), F.lit(0)), 3).alias("course_id_enrolled_at_date_year_avg_score_to_row"),
        F.max("score").alias("max_score"),
    )
)
