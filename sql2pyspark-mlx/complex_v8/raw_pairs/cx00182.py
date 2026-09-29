from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_credits_ties = Window.orderBy(F.col("credits").asc_nulls_last())
w_level_credits = Window.partitionBy("level", "credits")
w_all_by_course_code_len = Window.orderBy("course_code_len", "id")

scoped_courses = (
    df_courses
    .filter((F.col("credits") >= 2) & (F.col("level").isin("intro", "core")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
    )
)

screened = (
    scoped_courses.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.to_date(F.col("e.enrolled_at")) < F.lit("2025-02-18").cast("date")), "left_anti")
)

windowed = (
    screened
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_credits_ties), 3).alias("credits_cume_dist"),
        F.size(F.collect_set("dept").over(w_level_credits)).alias("distinct_dept_in_level_credits"),
        (F.col("credits") - F.lag("credits").over(w_all_by_course_code_len)).alias("credits_change"),
    )
)

result = (
    windowed
    .cube("dept", "level", "distinct_dept_in_level_credits")
    .agg(
        F.grouping("dept").alias("is_total_dept"),
        F.array_sort(F.collect_set("course_code")).alias("course_code_list"),
    )
    .orderBy(
        F.col("dept").asc_nulls_last(),
        F.col("level").asc_nulls_last(),
        F.col("distinct_dept_in_level_credits").asc_nulls_last(),
    )
)
