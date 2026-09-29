from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("level").orderBy(F.col("credits").desc(), F.col("id").desc())

cleaned_courses = (
    df_courses
    .filter(F.lower(F.col("course_code")).like("%30%"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.concat_ws(" / ", F.col("course_code"), F.col("level")).alias("course_code_level_label"),
        F.coalesce(F.col("dept"), F.lit("UNKNOWN")).alias("dept_or_unknown"),
    )
)

latest = (
    cleaned_courses
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupingSets(
        [["dept", "level"], ["dept"]],
        "dept", "level",
    )
    .agg(
        F.array_sort(F.collect_set("course_code")).alias("course_code_list"),
        F.countDistinct("level").alias("distinct_levels"),
        F.min("credits").alias("min_credits"),
    )
    .orderBy(F.col("dept").asc_nulls_last(), F.col("level").asc_nulls_last())
)
