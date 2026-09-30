from pyspark.sql import functions as F

cleaned_courses = (
    df_courses
    .filter(F.col("credits") > 2)
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("course_code")), " ", "_")).alias("course_code_slug"),
        F.when(F.col("dept").isin("Biz", "Art"), "group_a").when(F.col("dept") == "Bio", "group_b").otherwise("other").alias("dept_group"),
    )
)

labeled = (
    cleaned_courses
    .select(
        "*",
        F.length(F.trim(F.col("dept_group"))).alias("dept_group_len"),
        F.concat_ws(" / ", F.col("dept_group"), F.col("level")).alias("dept_group_level_label"),
        F.lower(F.trim(F.col("course_code_slug"))).alias("course_code_slug_clean"),
    )
)

result = (
    labeled
    .cube("dept", "dept_group_len")
    .agg(F.sum("dept_group_len").alias("total_dept_group_len"))
)
