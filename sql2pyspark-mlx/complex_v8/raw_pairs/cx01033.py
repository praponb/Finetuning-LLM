from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month_by_score_f = Window.partitionBy(F.month("enrolled_at")).orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month_by_enrolle = Window.partitionBy(F.month("enrolled_at")).orderBy("enrolled_at", "id")
w_all_by_second_enrolled_at_ru = Window.orderBy(F.col("second_enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_second_enrolled_at = Window.orderBy(F.col("second_enrolled_at").asc_nulls_last(), "id")
w_course_id = Window.partitionBy("course_id")
w_all_by_enrolled_at_percent_r = Window.orderBy("enrolled_at_percent_rank_minus_median", "id")

prepared = (
    df_enrollments
    .filter((~F.col("grade_status").isin("FAIL", "WITHDRAWN")) & (F.col("score") <= 69.6))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "course_id",
        "term_prefix",
        F.nth_value("enrolled_at", 2).over(w_enrolled_at_month_by_score_f).alias("second_enrolled_at"),
        F.round(F.percent_rank().over(w_enrolled_at_month_by_enrolle), 3).alias("enrolled_at_percent_rank"),
    )
)

course_id_stats = (
    with_windows
    .groupBy("course_id")
    .agg(F.percentile_approx("enrolled_at_percent_rank", 0.5).alias("median_enrolled_at_percent_rank"))
)

vs_group = (
    with_windows.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.enrolled_at_percent_rank") >= F.col("g.median_enrolled_at_percent_rank"))
    .select(
        "s.*",
        "g.median_enrolled_at_percent_rank",
        F.round(F.col("s.enrolled_at_percent_rank") - F.col("g.median_enrolled_at_percent_rank"), 2).alias("enrolled_at_percent_rank_minus_median"),
    )
)

with_trends = (
    vs_group
    .select(
        "id",
        "term_prefix",
        "second_enrolled_at",
        "enrolled_at_percent_rank",
        "median_enrolled_at_percent_rank",
        "enrolled_at_percent_rank_minus_median",
        F.last("second_enrolled_at", ignorenulls=True).over(w_all_by_second_enrolled_at_ru).alias("last_known_second_enrolled_at"),
        (F.col("enrolled_at_percent_rank") - F.lag("enrolled_at_percent_rank").over(w_all_by_second_enrolled_at)).alias("enrolled_at_percent_rank_change"),
        F.round(F.col("enrolled_at_percent_rank") / F.nullif(F.sum("enrolled_at_percent_rank").over(w_course_id), F.lit(0)), 4).alias("enrolled_at_percent_rank_share_of_course_id"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_percent_r), 3).alias("enrolled_at_percent_rank_minus_median_percent_ra"),
        F.row_number().over(w_all_by_second_enrolled_at).alias("second_enrolled_at_seq"),
    )
)

tagged = (
    with_trends
    .filter(F.col("enrolled_at_percent_rank_minus_median") < 0.2)
    .withColumn("term_prefix_len", F.length(F.trim(F.col("term_prefix"))))
)

result = (
    tagged
    .select("id", "term_prefix_len", "second_enrolled_at_seq")
    .unpivot(
        ids=["id"],
        values=["term_prefix_len", "second_enrolled_at_seq"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
