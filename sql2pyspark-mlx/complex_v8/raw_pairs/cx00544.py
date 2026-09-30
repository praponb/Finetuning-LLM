from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("level").orderBy(F.col("credits").desc(), F.col("id").desc())

cleaned_courses = (
    df_courses
    .filter((F.col("level") == "core") & (F.upper(F.trim(F.col("course_code"))).like("C%")))
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
    )
)

scored = (
    cleaned_courses
    .select(
        "*",
        F.concat_ws("-", F.col("course_code"), F.col("dept_slug")).alias("course_code_dept_slug_label"),
        F.length(F.trim(F.col("dept_slug"))).alias("dept_slug_len"),
    )
)

unique_rows = (
    scored
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_activity = (
    unique_rows
    .crossJoin(unique_rows.agg(F.min("credits").alias("overall_min_credits")))
    .filter(F.col("credits") > F.col("overall_min_credits"))
    .drop("overall_min_credits")
)

result = (
    with_activity
    .groupingSets(
        [["level", "dept_slug_len"], ["level"]],
        "level", "dept_slug_len",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.col("course_code").isNotNull(), F.col("credits")).otherwise(0)).alias("credits_course_code_is_not_null"),
    )
    .orderBy(
        F.col("level").asc_nulls_last(),
        F.col("dept_slug_len").asc_nulls_last(),
    )
)
