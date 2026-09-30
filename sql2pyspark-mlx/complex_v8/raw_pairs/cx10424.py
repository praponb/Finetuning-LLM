from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_dept_by_enrollment_ter = Window.partitionBy("level", "dept").orderBy("enrollment_term_count", "id")
w_level_dept_group_by_enrollme = Window.partitionBy("level", "dept_group").orderBy("enrollment_term_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level = Window.partitionBy("level")
w_level_dept_by_enrollment_ter_2 = Window.partitionBy("level", "dept").orderBy("enrollment_term_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_lower_course_code_like_00 = Window.partitionBy("n_lower_course_code_like_00")
w_n_lower_course_code_like_00_2 = Window.partitionBy("n_lower_course_code_like_00", "dept").orderBy("n_lower_course_code_like_00", "dept")
w_all_by_n_lower_course_code_l = Window.orderBy("n_lower_course_code_like_00", "dept").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_courses = (
    df_courses
    .filter((F.col("level") == "intro") & (F.lower(F.col("course_code")).like("%10%")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.when(F.col("dept").isin("Math", "Biz"), "group_a").when(F.col("dept") == "Bio", "group_b").otherwise("other").alias("dept_group"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count(F.when(F.col("term") == "2024T1", 1)).alias("enrollment_term_count"),
    )
)

summarized_rows = (
    scoped_courses.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

windowed = (
    summarized_rows
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        "dept_group",
        "last_enrollment_enrolled_at",
        (F.col("credits") - F.lag("credits").over(w_level_dept_by_enrollment_ter)).alias("credits_change"),
        F.nth_value("credits", 2).over(w_level_dept_group_by_enrollme).alias("second_credits"),
        F.round(F.col("credits") - F.avg("credits").over(w_level), 2).alias("credits_vs_level_avg"),
        F.rank().over(w_level_dept_by_enrollment_ter).alias("enrollment_term_count_rank"),
        F.last("credits").over(w_level_dept_by_enrollment_ter_2).alias("last_credits"),
        F.count(F.lit(1)).over(w_level).alias("rows_in_level"),
    )
)

rollup_base = (
    windowed
    .filter(F.col("enrollment_term_count_rank").between(3, 9))
    .groupBy("dept")
    .agg(
        F.sum(F.when(F.lower(F.col("course_code")).like("%00%"), 1).otherwise(0)).alias("n_lower_course_code_like_00"),
        F.max("rows_in_level").alias("max_rows_in_level"),
    )
)

screened = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("max_rows_in_level").alias("overall_avg_max_rows_in_level")))
    .filter(F.col("max_rows_in_level") > F.col("overall_avg_max_rows_in_level"))
    .drop("overall_avg_max_rows_in_level")
)

metrics = (
    screened
    .select(
        "dept",
        F.round(F.col("max_rows_in_level") / F.nullif(F.sum("max_rows_in_level").over(w_n_lower_course_code_like_00), F.lit(0)), 4).alias("max_rows_in_level_share_of_n_lower_course_code_l"),
        F.dense_rank().over(w_n_lower_course_code_like_00_2).alias("n_lower_course_code_like_00_dense_rank"),
        F.nth_value("max_rows_in_level", 2).over(w_all_by_n_lower_course_code_l).alias("second_max_rows_in_level"),
        F.size(F.collect_set("dept").over(w_n_lower_course_code_like_00)).alias("distinct_dept_in_n_lower_course_code_like_00"),
        F.sum("n_lower_course_code_like_00").over(w_n_lower_course_code_like_00).alias("n_lower_course_code_like_00_sum_n_lower_course_c"),
    )
)

result = (
    metrics
    .select(
        "dept",
        "max_rows_in_level_share_of_n_lower_course_code_l",
        "n_lower_course_code_like_00_dense_rank",
        "second_max_rows_in_level",
        "distinct_dept_in_n_lower_course_code_like_00",
        "n_lower_course_code_like_00_sum_n_lower_course_c",
        F.when(F.col("dept").isin("Math", "Bio"), "group_a").when(F.col("dept") == "Biz", "group_b").otherwise("other").alias("dept_group"),
        (F.floor(F.col("second_max_rows_in_level") / 5) * 5).alias("second_max_rows_in_level_bucket"),
    )
    .orderBy(F.col("dept").desc(), "dept_group")
)
