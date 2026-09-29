from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_course_level_by_p = Window.partitionBy("student_id", "course_level").orderBy("pct_course_level_is_not_null", "student_id", "course_level", "course_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_cnt = Window.partitionBy("student_id", "cnt")

recent_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

joined = (
    recent_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        F.col("c.level").alias("course_level"),
    )
)

totals = (
    joined
    .groupBy("student_id", "course_level", "course_id")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("course_level").isin("advanced", "core"), 1).otherwise(0)).alias("n_course_level_in_advanced_cor"),
        F.array_sort(F.collect_set("course_level")).alias("course_level_list"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.round(100 * F.count(F.when(F.col("course_level").isNotNull(), 1)) / F.count("*"), 1).alias("pct_course_level_is_not_null"),
    )
    .filter(F.col("cnt") >= 2)
)

with_trends = (
    totals
    .withColumn("last_pct_course_level_is_not_null", F.last("pct_course_level_is_not_null").over(w_student_id_course_level_by_p))
    .withColumn("cnt_share_of_student_id_cnt", F.round(F.col("cnt") / F.nullif(F.sum("cnt").over(w_student_id_cnt), F.lit(0)), 4))
)

result = (
    with_trends
    .select(
        "student_id",
        "course_level",
        "course_id",
        "cnt",
        "n_course_level_in_advanced_cor",
        "course_level_list",
        "distinct_course_ids",
        "last_pct_course_level_is_not_null",
        "cnt_share_of_student_id_cnt",
        F.least(F.col("cnt"), F.col("distinct_course_ids")).alias("least_cnt_distinct_course_ids"),
        F.abs(F.col("distinct_course_ids") - 4).alias("distinct_course_ids_distance"),
    )
    .orderBy(F.col("distinct_course_ids").desc())
)
