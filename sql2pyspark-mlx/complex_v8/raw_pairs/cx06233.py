from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_term = Window.partitionBy("course_id", "term")
w_all_by_enrolled_at_2 = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc())

filtered_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("score") <= 59.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.concat_ws("-", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

metrics = (
    filtered_enrollments
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "term_slug",
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        (F.col("score") - F.lag("score").over(w_all_by_enrolled_at)).alias("score_change"),
        F.lag("enrolled_at", 3).over(w_all_by_enrolled_at).alias("prev3_enrolled_at"),
        F.first("enrolled_at").over(w_all_by_enrolled_at_full).alias("first_enrolled_at"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id_term), F.lit(0)), 4).alias("score_share_of_course_id_term"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
        F.round(F.col("score") - F.avg("score").over(w_course_id_term), 2).alias("score_vs_course_id_term_avg"),
        F.sum("score").over(w_course_id).alias("course_id_sum_score"),
        F.lead("score", 3, 0).over(w_all_by_enrolled_at_2).alias("next3_score"),
        (F.col("score") - F.lag("score").over(w_course_id_by_enrolled_at)).alias("score_change_2"),
        F.max("score").over(w_course_id).alias("course_id_max_score"),
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank"),
        F.lead("score", 3, 0).over(w_course_id_by_enrolled_at).alias("next3_score_2"),
    )
)

top_per_group = metrics.filter(F.col("enrolled_at_rank") <= 3)

stats = (
    top_per_group
    .filter(F.col("score_share_of_course_id_term") >= 0.2)
    .groupBy("grade_status", "course_id")
    .agg(
        F.count(F.when(~F.col("grade_status").isin("FAIL", "PASS"), 1)).alias("n_grade_status_not_in_fail_pas"),
        F.sum(F.when(F.col("grade_status").isNotNull(), 1).otherwise(0)).alias("n_grade_status_is_not_null"),
    )
)

result = (
    stats
    .filter(F.col("course_id").isNotNull())
    .select(
        "grade_status",
        "n_grade_status_not_in_fail_pas",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
        F.when(F.col("grade_status").isin("WITHDRAWN", "FAIL"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
    )
    .orderBy("grade_status_prefix", F.col("grade_status").desc())
)
