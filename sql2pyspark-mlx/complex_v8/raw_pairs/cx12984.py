from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_plus_15m_month_b = Window.partitionBy(F.date_trunc("month", "enrolled_at_plus_15m")).orderBy(F.col("term_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_plus_15m_month_b_2 = Window.partitionBy(F.date_trunc("month", "enrolled_at_plus_15m")).orderBy(F.col("term_len").desc())
w_enrolled_at_year = Window.partitionBy(F.year("enrolled_at"))
w_enrolled_at_plus_15m_month_b_3 = Window.partitionBy(F.date_trunc("month", "enrolled_at_plus_15m")).orderBy(F.col("term_len").desc(), "id")
w_enrolled_at_plus_15m_month_b_4 = Window.partitionBy(F.date_trunc("month", "enrolled_at_plus_15m")).orderBy(F.col("term_len").desc(), "id").rowsBetween(-1, Window.currentRow)

valid_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.abs(F.col("score") - 58.8).alias("score_distance"),
    )
)

tagged = (
    valid_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "*",
        F.length(F.trim(F.col("term"))).alias("term_len_2"),
        (F.col("enrolled_at") + F.expr("INTERVAL 15 MINUTES")).alias("enrolled_at_plus_15m"),
        F.round(F.col("score_distance") * 0.25, 2).alias("score_distance_adj"),
    )
)

with_trends = (
    tagged
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "term_len_2",
        "enrolled_at_plus_15m",
        F.sum("term_len_2").over(w_enrolled_at_plus_15m_month_b).alias("running_term_len_2"),
        F.rank().over(w_enrolled_at_plus_15m_month_b_2).alias("term_len_rank"),
        F.count(F.lit(1)).over(w_enrolled_at_year).alias("rows_in_enrolled_at_year"),
        (F.col("score_distance") - F.lag("score_distance").over(w_enrolled_at_plus_15m_month_b_3)).alias("score_distance_change"),
        F.round(F.avg("term_len_2").over(w_enrolled_at_plus_15m_month_b_4), 2).alias("term_len_2_avg_2"),
        F.size(F.collect_set("term").over(w_enrolled_at_year)).alias("distinct_term_in_enrolled_at_year"),
    )
)

joined_course = (
    with_trends.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    joined_course
    .groupingSets(
        [["course_credits", "course_dept", "distinct_term_in_enrolled_at_year"], ["course_credits"]],
        "course_credits", "course_dept", "distinct_term_in_enrolled_at_year",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("score_distance", 0.5).alias("median_score_distance"),
    )
    .orderBy(
        F.col("course_credits").asc_nulls_last(),
        F.col("course_dept").asc_nulls_last(),
        F.col("distinct_term_in_enrolled_at_year").asc_nulls_last(),
    )
)
