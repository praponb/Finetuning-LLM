from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits_distance_level_by_de = Window.partitionBy("credits_distance", "level").orderBy(F.col("dept_len_per_credits_distance").desc(), "id")
w_dept_len_dept = Window.partitionBy("dept_len", "dept")
w_credits_distance_level_by_de_2 = Window.partitionBy("credits_distance", "level").orderBy(F.col("dept_len_per_credits_distance").desc(), "id").rowsBetween(-6, Window.currentRow)

scoped_courses = (
    df_courses
    .filter((F.length(F.trim(F.col("course_code"))) >= 5) & (F.col("credits").between(2, 4)))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.concat(F.col("course_code"), F.lit("#"), F.col("credits").cast("string")).alias("course_code_ref"),
        F.abs(F.col("credits") - 2).alias("credits_distance"),
        F.length(F.trim(F.col("dept"))).alias("dept_len"),
    )
)

labeled = (
    scoped_courses
    .filter(F.col("credits") <= 3)
    .select(
        "*",
        F.substring(F.trim(F.col("course_code")), 1, 2).alias("course_code_prefix"),
        F.round(F.col("dept_len") / F.nullif(F.col("credits_distance"), F.lit(0)), 3).alias("dept_len_per_credits_distance"),
        F.when(F.col("dept").isin("Bio", "Biz"), "group_a").when(F.col("dept") == "CS", "group_b").otherwise("other").alias("dept_group"),
    )
)

with_windows = (
    labeled
    .select(
        "*",
        (F.col("dept_len") - F.lag("dept_len").over(w_credits_distance_level_by_de)).alias("dept_len_change"),
        F.round(F.col("credits") / F.nullif(F.sum("credits").over(w_dept_len_dept), F.lit(0)), 4).alias("credits_share_of_dept_len_dept"),
        F.round(F.avg("dept_len").over(w_credits_distance_level_by_de_2), 2).alias("dept_len_avg_7"),
    )
)

filtered = (
    with_windows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.score") > 38.3), "left_anti")
)

filtered_2 = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left_semi")
    .filter(F.lower(F.col("course_code")).like("%05%"))
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.min("score").alias("min_enrollment_score"))
)

profiled = (
    filtered_2.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select("s.*", "e.min_enrollment_score")
)

result = (
    profiled
    .groupBy("level")
    .pivot("dept", ["CS", "Math"])
    .agg(F.sum("credits_share_of_dept_len_dept"))
)
