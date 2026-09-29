from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_level_student_id_by_c = Window.partitionBy("course_level", "student_id").orderBy(F.col("course_credits").asc_nulls_last(), "id")
w_course_level = Window.partitionBy("course_level")
w_course_level_student_id_by_c_2 = Window.partitionBy("course_level", "student_id").orderBy(F.col("course_credits").asc_nulls_last())
w_course_credits_quintile_cour = Window.partitionBy("course_credits_quintile", "course_level")
w_course_credits_quintile_by_s = Window.partitionBy("course_credits_quintile").orderBy(F.col("stddev_course_credits").desc(), "course_credits_quintile", "course_level").rowsBetween(-1, Window.currentRow)
w_course_credits_quintile_by_s_2 = Window.partitionBy("course_credits_quintile").orderBy(F.col("stddev_course_credits").desc(), "course_credits_quintile", "course_level")
w_course_credits_quintile_by_s_3 = Window.partitionBy("course_credits_quintile").orderBy(F.col("stddev_course_credits").desc(), "course_credits_quintile", "course_level").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_level_course_credits = Window.partitionBy("course_level", "course_credits_quintile").orderBy("n_rows")
w_all_by_last_course_credits_q = Window.orderBy("last_course_credits_quintile", "course_credits_quintile", "course_level").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows_course_level = Window.partitionBy("n_rows", "course_level")
w_n_rows_by_n_rows = Window.partitionBy("n_rows").orderBy("n_rows", "course_credits_quintile", "course_level")
w_n_rows_by_n_rows_ties = Window.partitionBy("n_rows").orderBy("n_rows")

cleaned = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("term").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.credits").between(2, 4))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.enrolled_at_date",
        "s.grade_status_clean",
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

windowed = (
    with_parent
    .select(
        "*",
        F.round(F.cume_dist().over(w_course_level_student_id_by_c), 3).alias("course_credits_cume_dist"),
        F.count(F.lit(1)).over(w_course_level).alias("rows_in_course_level"),
        F.ntile(5).over(w_course_level_student_id_by_c).alias("course_credits_quintile"),
        F.sum("score").over(w_course_level).alias("course_level_sum_score"),
        F.round(F.cume_dist().over(w_course_level_student_id_by_c_2), 3).alias("course_credits_cume_dist_2"),
    )
)

grouped = (
    windowed
    .filter(F.col("term") == "2024T1")
    .groupBy("course_credits_quintile", "course_level")
    .agg(
        F.round(F.stddev("course_credits"), 2).alias("stddev_course_credits"),
        F.count("*").alias("n_rows"),
        F.sum("course_credits_cume_dist").alias("sum_course_credits_cume_dist_check"),
    )
    .filter(F.col("sum_course_credits_cume_dist_check") > 0.4)
    .drop("sum_course_credits_cume_dist_check")
)

metrics = (
    grouped
    .select(
        "course_credits_quintile",
        "course_level",
        "stddev_course_credits",
        "n_rows",
        F.count(F.lit(1)).over(w_course_credits_quintile_cour).alias("rows_in_course_credits_quintile_course_level"),
        F.round(F.avg("n_rows").over(w_course_credits_quintile_by_s), 2).alias("n_rows_avg_2"),
        F.round(F.col("stddev_course_credits") - F.avg("stddev_course_credits").over(w_course_credits_quintile_cour), 2).alias("stddev_course_credits_vs_course_credits_quintile"),
        F.round(F.cume_dist().over(w_course_credits_quintile_by_s_2), 3).alias("stddev_course_credits_cume_dist"),
        F.last("course_credits_quintile").over(w_course_credits_quintile_by_s_3).alias("last_course_credits_quintile"),
        F.round(F.cume_dist().over(w_course_level_course_credits), 3).alias("n_rows_cume_dist"),
    )
)

with_trends = (
    metrics
    .select(
        "course_credits_quintile",
        "course_level",
        "stddev_course_credits",
        "n_rows",
        "rows_in_course_credits_quintile_course_level",
        "n_rows_avg_2",
        "stddev_course_credits_vs_course_credits_quintile",
        "last_course_credits_quintile",
        F.sum("n_rows_avg_2").over(w_all_by_last_course_credits_q).alias("running_n_rows_avg_2"),
        F.round(F.col("last_course_credits_quintile") - F.avg("last_course_credits_quintile").over(w_n_rows_course_level), 2).alias("last_course_credits_quintile_vs_n_rows_course_le"),
        F.lag("last_course_credits_quintile", 1, 0).over(w_n_rows_by_n_rows).alias("prev_last_course_credits_quintile"),
        F.lead("n_rows_avg_2").over(w_n_rows_by_n_rows).alias("next_n_rows_avg_2"),
        F.dense_rank().over(w_n_rows_by_n_rows_ties).alias("n_rows_dense_rank"),
    )
)

top_rows = (
    with_trends
    .filter((F.col("n_rows_dense_rank") == 1) & (F.col("prev_last_course_credits_quintile") > 3))
)

result = (
    top_rows
    .select(
        "course_credits_quintile",
        "course_level",
        "stddev_course_credits",
        "n_rows",
        "rows_in_course_credits_quintile_course_level",
        "last_course_credits_quintile",
        "last_course_credits_quintile_vs_n_rows_course_le",
        "prev_last_course_credits_quintile",
        "next_n_rows_avg_2",
        "n_rows_dense_rank",
        F.lower(F.trim(F.col("course_level"))).alias("course_level_clean"),
        (F.col("n_rows") + F.col("n_rows_avg_2")).alias("n_rows_plus_n_rows_avg_2"),
    )
    .orderBy(
        F.col("last_course_credits_quintile_vs_n_rows_course_le").desc_nulls_first(),
        F.col("rows_in_course_credits_quintile_course_level").desc(),
    )
)
