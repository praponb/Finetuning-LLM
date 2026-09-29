from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("term", "grade_status").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

recent_enrollments = (
    df_enrollments
    .filter((F.col("course_id") >= 4) & (~F.col("term").isin("2025T2", "2024T1")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 57.6).alias("score_distance"),
    )
)

deduped = (
    recent_enrollments
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

qualified = (
    deduped.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") <= 57.0), "left_anti")
)

qualified_2 = (
    qualified
    .crossJoin(qualified.agg(F.avg("score").alias("overall_avg_score")))
    .filter(F.col("score") > F.col("overall_avg_score"))
    .drop("overall_avg_score")
)

result = (
    qualified_2
    .rollup("grade_status", "course_id", "student_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("score"), 2).alias("total_score"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
    )
    .select(
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "course_id",
        "student_id",
        "grouping_level",
        "total_score",
        "grade_status_list",
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
