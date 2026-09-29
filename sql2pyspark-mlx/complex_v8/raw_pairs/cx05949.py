from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("term", "grade_status").orderBy(F.col("grade_status_len").desc(), F.col("id").desc())
w_course_id_by_score = Window.partitionBy("course_id").orderBy("score", "id")
w_grade_status_grade_status_le = Window.partitionBy("grade_status", "grade_status_len")

filtered = (
    df_enrollments
    .filter(~F.col("grade_status").isin("WITHDRAWN", "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
        F.abs(F.col("score") - 57.8).alias("score_distance"),
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.concat_ws(": ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

unique_rows = (
    enriched
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_trends = (
    unique_rows
    .withColumn("prev_score_distance", F.lag("score_distance").over(w_course_id_by_score))
    .withColumn("score_share_of_grade_status_grade_status_len", F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_grade_status_le), F.lit(0)), 4))
)

per_group = (
    with_trends
    .filter(F.col("course_id").isNotNull())
    .groupBy("course_id", "term", "enrolled_at_year")
    .agg(
        F.round(100 * F.count(F.when(F.col("term") == "2024T1", 1)) / F.count("*"), 1).alias("pct_term_2024t1"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.sum("grade_status_len").alias("total_grade_status_len"),
        F.round(F.avg("grade_status_len"), 2).alias("avg_grade_status_len"),
    )
)

tagged = (
    per_group
    .select(
        "*",
        F.round(F.col("avg_grade_status_len") * 0.25, 2).alias("avg_grade_status_len_adj"),
        F.abs(F.col("enrolled_at_year") - 2025).alias("enrolled_at_year_distance"),
        F.round(F.col("avg_grade_status_len") * 1.07, 2).alias("avg_grade_status_len_adj_2"),
    )
)

result = (
    tagged
    .select(
        "course_id",
        "term",
        "enrolled_at_year",
        "grade_status_list",
        "total_grade_status_len",
        "avg_grade_status_len",
        "enrolled_at_year_distance",
        "avg_grade_status_len_adj_2",
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)
