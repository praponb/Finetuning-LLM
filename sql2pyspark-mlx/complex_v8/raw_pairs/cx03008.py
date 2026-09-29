from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_term_by_enrolled = Window.partitionBy("student_id", "term").orderBy("enrolled_at")
w_student_id_term_by_enrolled_2 = Window.partitionBy("student_id", "term").orderBy("enrolled_at", "id")
w_student_id_course_id_by_enro = Window.partitionBy("student_id", "course_id").orderBy(F.col("enrolled_at").desc(), "id")
w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at")
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_student_id_term_by_enrolled_3 = Window.partitionBy("student_id", "term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
    )
)

with_trends = (
    recent
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "grade_status_clean",
        F.dense_rank().over(w_student_id_term_by_enrolled).alias("enrolled_at_dense_rank"),
        F.lag("enrolled_at").over(w_student_id_term_by_enrolled_2).alias("prev_enrolled_at"),
        F.ntile(5).over(w_student_id_course_id_by_enro).alias("enrolled_at_quintile"),
        F.round(F.percent_rank().over(w_term_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
        F.dense_rank().over(w_grade_status_by_enrolled_at).alias("enrolled_at_dense_rank_3"),
        F.row_number().over(w_student_id_term_by_enrolled_2).alias("enrolled_at_seq"),
        F.ntile(10).over(w_term_by_enrolled_at).alias("enrolled_at_decile"),
        F.rank().over(w_grade_status_by_enrolled_at).alias("enrolled_at_rank"),
        F.row_number().over(w_grade_status_by_enrolled_at_2).alias("rn"),
        F.last("enrolled_at").over(w_student_id_term_by_enrolled_3).alias("last_enrolled_at"),
        F.nth_value("enrolled_at", 2).over(w_student_id_term_by_enrolled_3).alias("second_enrolled_at"),
        F.round(F.percent_rank().over(w_grade_status_by_enrolled_at), 3).alias("enrolled_at_percent_rank_2"),
    )
)

top_per_group = (
    with_trends
    .filter(F.col("rn") <= 2)
    .drop("rn")
)

summary = (
    top_per_group
    .filter(F.col("enrolled_at_rank") >= 4)
    .groupBy(
        F.when(F.col("enrolled_at_percent_rank") >= 0.7, "large").when(F.col("enrolled_at_percent_rank") >= 0.4, "medium").otherwise("small").alias("enrolled_at_percent_rank_tier"),
        F.date_format("enrolled_at", "yyyy-MM").alias("enrolled_at_ym"),
        "enrolled_at_seq",
    )
    .agg(
        F.count(F.when(F.col("grade_status") == "FAIL", 1)).alias("n_grade_status_fail"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.round(F.sum("enrolled_at_percent_rank_2") / F.nullif(F.sum("enrolled_at_dense_rank"), F.lit(0)), 3).alias("enrolled_at_percent_rank_2_to_enrolled_at_dense_"),
        F.max("enrolled_at_dense_rank").alias("max_enrolled_at_dense_rank_check"),
    )
    .filter(F.col("max_enrolled_at_dense_rank_check") > 3)
    .drop("max_enrolled_at_dense_rank_check")
)

result = (
    summary
    .select(
        "enrolled_at_percent_rank_tier",
        "enrolled_at_seq",
        "n_grade_status_fail",
        F.substring(F.trim(F.col("enrolled_at_ym")), 1, 4).alias("enrolled_at_ym_prefix"),
        F.length(F.trim(F.col("enrolled_at_ym"))).alias("enrolled_at_ym_len"),
    )
    .orderBy(F.col("enrolled_at_ym_len").desc_nulls_first())
)
