from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_len_by_level_len = Window.partitionBy("level_len").orderBy("level_len", "id")
w_credits = Window.partitionBy("credits")
w_credits_by_level_len_running = Window.partitionBy("credits").orderBy(F.col("level_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_courses = (
    df_courses
    .filter((F.col("credits") >= 2) & (F.col("dept") == "Bio"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
        F.lower(F.trim(F.col("dept"))).alias("dept_clean"),
        F.length(F.trim(F.col("level"))).alias("level_len"),
    )
)

with_trends = (
    scoped_courses
    .select(
        "*",
        F.lead("credits_filled").over(w_level_len_by_level_len).alias("next_credits_filled"),
        F.round(F.col("credits_filled") - F.avg("credits_filled").over(w_credits), 2).alias("credits_filled_vs_credits_avg"),
        F.sum("credits_filled").over(w_credits_by_level_len_running).alias("running_credits_filled"),
    )
)

result = (
    with_trends
    .cube("credits", "level_len", "credits_filled")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("running_credits_filled") / F.nullif(F.sum("next_credits_filled"), F.lit(0)), 3).alias("running_credits_filled_to_next_credits_filled_ra"),
        F.array_sort(F.collect_set("course_code")).alias("course_code_list"),
    )
)
