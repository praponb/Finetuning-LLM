from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dept_by_credits_moving_1 = Window.partitionBy("dept").orderBy("credits", "id").rowsBetween(-1, Window.currentRow)
w_dept_by_credits = Window.partitionBy("dept").orderBy("credits", "id")
w_credits_level_by_credits = Window.partitionBy("credits", "level").orderBy("credits", "id")
w_level = Window.partitionBy("level")

scoped = (
    df_courses
    .filter(F.upper(F.trim(F.col("course_code"))).like("C%"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.round(F.avg("credits").over(w_dept_by_credits_moving_1), 2).alias("credits_avg_2"),
        F.lag("credits", 3, 0).over(w_dept_by_credits).alias("prev3_credits"),
        F.dense_rank().over(w_credits_level_by_credits).alias("credits_dense_rank"),
        F.dense_rank().over(w_dept_by_credits).alias("credits_dense_rank_2"),
        F.max("credits").over(w_level).alias("level_max_credits"),
        F.round(F.percent_rank().over(w_dept_by_credits), 3).alias("credits_percent_rank"),
    )
)

dept_stats = (
    metrics
    .groupBy("dept")
    .agg(F.max("credits_dense_rank").alias("max_credits_dense_rank"))
)

outliers = (
    metrics.alias("s")
    .join(dept_stats.alias("g"), F.col("g.dept") == F.col("s.dept"), "inner")
    .filter(F.col("s.credits_dense_rank") == F.col("g.max_credits_dense_rank"))
    .select(
        "s.*",
        "g.max_credits_dense_rank",
        F.round(F.col("s.credits_dense_rank") - F.col("g.max_credits_dense_rank"), 2).alias("credits_dense_rank_minus_max"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

with_counts = (
    outliers.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
        "e.first_enrollment_enrolled_at",
    )
)

result = (
    with_counts
    .groupBy("credits")
    .pivot("dept", ["CS", "Biz", "Art", "Bio"])
    .agg(F.count("prev3_credits"))
    .orderBy("credits")
)
