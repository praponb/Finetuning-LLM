from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at_quarter").desc(), "id")
w_course_id_grade_status_by_sc = Window.partitionBy("course_id", "grade_status").orderBy("score_distance_adj_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_quarter_p = Window.orderBy("enrolled_at_quarter", "id").rowsBetween(-2, -1)
w_term_by_min_score_distance_p = Window.partitionBy("term").orderBy("min_score_distance", "term").rowsBetween(-6, -1)
w_term_by_min_score_distance = Window.partitionBy("term").orderBy("min_score_distance", "term")
w_term = Window.partitionBy("term")

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 38.1).alias("score_distance"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
    )
)

annotated = (
    cleaned_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "*",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.round(F.col("score_distance") * 0.25, 2).alias("score_distance_adj"),
        F.round(F.col("score_distance") * 0.9, 2).alias("score_distance_adj_2"),
    )
)

sequenced = (
    annotated
    .select(
        "*",
        F.ntile(10).over(w_grade_status_by_enrolled_at).alias("enrolled_at_quarter_decile"),
        F.last("score_distance_adj_2").over(w_course_id_grade_status_by_sc).alias("last_score_distance_adj_2"),
        F.max("score_distance_adj_2").over(w_all_by_enrolled_at_quarter_p).alias("score_distance_adj_2_max_2_prior"),
    )
)

stats = (
    sequenced
    .filter(F.col("enrolled_at_quarter").between(2, 3))
    .groupBy("term")
    .agg(
        F.min("score_distance").alias("min_score_distance"),
        F.max("last_score_distance_adj_2").alias("max_last_score_distance_adj_2"),
    )
)

windowed = (
    stats
    .select(
        "term",
        "max_last_score_distance_adj_2",
        F.round(F.avg("min_score_distance").over(w_term_by_min_score_distance_p), 2).alias("min_score_distance_avg_6_prior"),
        F.lag("min_score_distance").over(w_term_by_min_score_distance).alias("prev_min_score_distance"),
        F.round(F.avg("min_score_distance").over(w_term), 2).alias("term_avg_min_score_distance"),
    )
)

enriched = (
    windowed
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.upper(F.trim(F.col("term"))).alias("term_clean"),
    )
)

enriched_2 = (
    enriched
    .filter(F.col("term_slug").isNotNull())
    .withColumn("prev_min_score_distance_distance", F.abs(F.col("prev_min_score_distance") - 40.0))
)

result = (
    enriched_2
    .filter(F.col("term_slug").isNotNull())
    .select(
        "term",
        "min_score_distance_avg_6_prior",
        "prev_min_score_distance",
        "term_avg_min_score_distance",
        "term_slug",
        "prev_min_score_distance_distance",
        F.lower(F.trim(F.col("term"))).alias("term_clean_2"),
    )
    .orderBy("term_slug")
)
