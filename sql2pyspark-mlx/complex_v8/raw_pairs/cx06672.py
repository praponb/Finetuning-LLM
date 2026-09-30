from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("course_id", "student_id").orderBy(F.col("score").desc(), F.col("id").desc())

recent_enrollments = (
    df_enrollments
    .filter((F.col("course_id") >= 9) | (F.col("enrolled_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.when(F.col("term").isin("2024T1", "2024T2"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
    )
)

term_group_stats = (
    recent_enrollments
    .groupBy("term_group")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

compared = (
    recent_enrollments.alias("s")
    .join(term_group_stats.alias("g"), F.col("g.term_group") == F.col("s.term_group"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

with_activity = (
    compared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term").isin("2025T1", "2024T2")) & (F.col("e.student_id").isNotNull()), "left_semi")
    .filter(F.col("term_group") == "group_a")
)

latest = (
    with_activity
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "enrolled_at_label",
        "term_group",
        "score_minus_median",
        F.lower(F.regexp_replace(F.trim(F.col("enrolled_at_label")), " ", "_")).alias("enrolled_at_label_slug"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label_2"),
    )
    .orderBy(F.col("enrolled_at_label").desc_nulls_first(), F.col("course_id").asc())
)
