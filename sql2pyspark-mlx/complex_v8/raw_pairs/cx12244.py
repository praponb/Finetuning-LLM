from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("course_id").orderBy(F.col("score").desc(), F.col("id").desc())

cleaned_enrollments = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(7, 9))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.when(F.col("term").isin("2024T2", "2024T1"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
    )
)

unique_rows = (
    cleaned_enrollments
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    unique_rows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_year",
        "s.term_group",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    enriched
    .rollup("course_credits", "course_id", "grade_status")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.avg("score"), 2).alias("avg_score"),
        F.array_sort(F.collect_set("course_dept")).alias("course_dept_list"),
    )
    .orderBy(
        F.col("course_credits").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
