from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_dayofweek_by_enr = Window.partitionBy("enrolled_at_dayofweek").orderBy(F.col("enrolled_at").desc(), "id")
w_enrolled_at_to_date = Window.partitionBy(F.to_date("enrolled_at"))
w_enrolled_at_dayofweek_by_enr_2 = Window.partitionBy("enrolled_at_dayofweek").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id = Window.partitionBy("student_id")
w_enrolled_at_month_by_enrolle = Window.partitionBy(F.date_trunc("month", "enrolled_at")).orderBy("enrolled_at_dayofweek")
w_enrolled_at_dayofweek_by_max = Window.partitionBy("enrolled_at_dayofweek").orderBy("max_score_minus_median_so_far")

scoped_enrollments = (
    df_enrollments
    .filter((F.col("score") < 59.8) & (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

joined_course = (
    scoped_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(~F.col("c.level").isin("advanced", "core"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_dayofweek",
        "s.score_filled",
        "s.grade_status_term_label",
        F.col("c.level").alias("course_level"),
        "c.course_code",
    )
)

student_id_stats = (
    joined_course
    .groupBy("student_id")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

outliers = (
    joined_course.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

enriched_course = (
    outliers.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

metrics = (
    enriched_course
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        "enrolled_at_dayofweek",
        "score_filled",
        "score_minus_median",
        "course_dept",
        "course_credits",
        F.lead("enrolled_at_dayofweek").over(w_enrolled_at_dayofweek_by_enr).alias("next_enrolled_at_dayofweek"),
        (F.col("score_minus_median") - F.lag("score_minus_median").over(w_enrolled_at_dayofweek_by_enr)).alias("score_minus_median_change"),
        F.count(F.lit(1)).over(w_enrolled_at_to_date).alias("rows_in_enrolled_at_to_date"),
        F.sum("enrolled_at_dayofweek").over(w_enrolled_at_dayofweek_by_enr_2).alias("running_enrolled_at_dayofweek"),
        F.max("score_minus_median").over(w_enrolled_at_dayofweek_by_enr_2).alias("max_score_minus_median_so_far"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.min("rows_in_enrolled_at_to_date").over(w_student_id).alias("student_id_min_rows_in_enrolled_at_to_date"),
        F.round(F.col("rows_in_enrolled_at_to_date") - F.avg("rows_in_enrolled_at_to_date").over(w_student_id), 2).alias("rows_in_enrolled_at_to_date_vs_student_id_avg"),
        F.round(F.percent_rank().over(w_enrolled_at_month_by_enrolle), 3).alias("enrolled_at_dayofweek_percent_rank"),
        F.dense_rank().over(w_enrolled_at_dayofweek_by_max).alias("max_score_minus_median_so_far_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "enrolled_at_dayofweek",
        "score_filled",
        "score_minus_median",
        "course_dept",
        "course_credits",
        "next_enrolled_at_dayofweek",
        "rows_in_enrolled_at_to_date",
        "running_enrolled_at_dayofweek",
        "max_score_minus_median_so_far",
        "rows_in_enrolled_at_to_date_vs_student_id_avg",
        "enrolled_at_dayofweek_percent_rank",
        "max_score_minus_median_so_far_dense_rank",
        F.greatest(F.col("course_credits"), F.col("score")).alias("greatest_course_credits_score"),
        F.lower(F.trim(F.col("course_dept"))).alias("course_dept_clean"),
    )
    .orderBy(F.col("course_dept_clean").desc(), "student_id")
)
