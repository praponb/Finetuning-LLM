from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_credits = Window.orderBy(F.col("credits").desc(), "id")
w_level = Window.partitionBy("level")
w_all_by_rows_in_level = Window.orderBy(F.col("rows_in_level").desc(), "id")
w_all_by_rows_in_level_ties = Window.orderBy(F.col("rows_in_level").desc())

filtered_courses = (
    df_courses
    .filter((F.col("level").isin("advanced", "intro")) & (F.col("credits") > 2))
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("level")), " ", "_")).alias("level_slug"),
    )
)

filtered = (
    filtered_courses.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left_semi")
)

windowed = (
    filtered
    .select(
        "*",
        F.rank().over(w_all_by_credits).alias("credits_rank"),
        F.min("credits").over(w_level).alias("level_min_credits"),
        F.count(F.lit(1)).over(w_level).alias("rows_in_level"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.lead("rows_in_level").over(w_all_by_rows_in_level).alias("next_rows_in_level"),
        F.dense_rank().over(w_all_by_rows_in_level_ties).alias("rows_in_level_dense_rank"),
    )
)

result = (
    windowed_2
    .cube("level", "rows_in_level_dense_rank")
    .agg(
        F.grouping("level").alias("is_total_level"),
        F.array_sort(F.collect_set("level")).alias("level_list"),
        F.round(F.sum("credits") / F.nullif(F.sum("next_rows_in_level"), F.lit(0)), 3).alias("credits_to_next_rows_in_level_ratio"),
    )
    .select(
        F.coalesce(F.col("level"), F.lit("ALL")).alias("level"),
        "rows_in_level_dense_rank",
        "is_total_level",
        "level_list",
        "credits_to_next_rows_in_level_ratio",
    )
    .orderBy(
        F.col("level").asc_nulls_last(),
        F.col("rows_in_level_dense_rank").asc_nulls_last(),
    )
)
