from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_group_course_id = Window.partitionBy("term_group", "course_id")
w_term_by_score = Window.partitionBy("term").orderBy("score", "id")

valid_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-24 09:00:00").cast("timestamp")) | (F.col("term").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

annotated = (
    valid_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
        F.when(F.col("term").isin("2025T2", "2024T2"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
    )
)

filtered = (
    annotated.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.month(F.col("e.enrolled_at")).isin(1, 2)), "left_anti")
)

enriched = (
    filtered
    .filter(F.col("enrolled_at_label").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "term_group",
        F.concat_ws("|", F.col("term"), F.col("term_group")).alias("term_term_group_label"),
    )
)

tagged = (
    enriched
    .filter(F.col("score") < 63.9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "term_group",
        "term_term_group_label",
        F.when(F.col("term").isin("2025T1", "2024T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group_2"),
    )
)

with_trends = (
    tagged
    .select(
        "*",
        F.count(F.lit(1)).over(w_term_group_course_id).alias("rows_in_term_group_course_id"),
        F.round(F.avg("score").over(w_term_group_course_id), 2).alias("term_group_course_id_avg_score"),
        F.min("score").over(w_term_group_course_id).alias("term_group_course_id_min_score"),
        (F.col("score") - F.lag("score").over(w_term_by_score)).alias("score_change"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term_group_course_id), F.lit(0)), 4).alias("score_share_of_term_group_course_id"),
    )
)

result = (
    with_trends
    .filter(F.col("score").between(20.6, 92.4))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "term_group",
        "term_term_group_label",
        "rows_in_term_group_course_id",
        "term_group_course_id_avg_score",
        "term_group_course_id_min_score",
        "score_share_of_term_group_course_id",
    )
    .orderBy(F.col("student_id").asc())
)
