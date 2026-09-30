from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_course_code_len_full = Window.orderBy("course_code_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_code_len = Window.partitionBy("course_code_len")

base = (
    df_courses
    .filter((F.col("level") == "advanced") & (F.col("credits") < 3))
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        F.when(F.col("credits") >= 3, "A").when(F.col("credits") >= 2, "B").otherwise("C").alias("credits_tier"),
    )
)

scored = (
    base
    .filter(F.col("level") == "advanced")
    .select(
        "id",
        "course_code",
        "dept",
        "credits_tier",
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
        F.concat_ws(": ", F.col("level"), F.col("dept")).alias("level_dept_label"),
        F.substring(F.trim(F.col("level")), 1, 4).alias("level_prefix"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "course_code",
        "level_dept_label",
        F.nth_value("course_code_len", 2).over(w_all_by_course_code_len_full).alias("second_course_code_len"),
        F.round(F.col("course_code_len") / F.nullif(F.sum("course_code_len").over(w_course_code_len), F.lit(0)), 4).alias("course_code_len_share_of_course_code_len"),
        F.size(F.collect_set("credits_tier").over(w_course_code_len)).alias("distinct_credits_tier_in_course_code_len"),
        F.round(F.col("course_code_len") - F.avg("course_code_len").over(w_course_code_len), 2).alias("course_code_len_vs_course_code_len_avg"),
    )
)

result = (
    metrics
    .filter(F.col("level_dept_label").isNotNull())
    .select(
        "id",
        "course_code",
        "level_dept_label",
        "second_course_code_len",
        "course_code_len_vs_course_code_len_avg",
        F.substring(F.trim(F.col("course_code")), 1, 3).alias("course_code_prefix"),
    )
    .orderBy(F.col("second_course_code_len").desc())
)
