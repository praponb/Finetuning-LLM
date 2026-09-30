from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_days_since_enrolled_a = Window.orderBy("days_since_enrolled_at", "id")
w_student_id = Window.partitionBy("student_id")
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc())

valid_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

with_activity = (
    valid_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status").isNotNull()), "left_semi")
)

tagged = (
    with_activity
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

scored = tagged.withColumn("enrolled_at_date", F.to_date(F.col("enrolled_at")))

metrics = (
    scored
    .select(
        "*",
        F.row_number().over(w_grade_status_by_enrolled_at).alias("enrolled_at_seq"),
        F.datediff(F.col("enrolled_at_date"), F.lag("enrolled_at_date").over(w_all_by_days_since_enrolled_a)).alias("days_since_prev_enrolled_at_date"),
        F.size(F.collect_set("course_id").over(w_student_id)).alias("distinct_course_id_in_student_id"),
        F.rank().over(w_grade_status_by_enrolled_at_2).alias("enrolled_at_rank"),
        F.round(F.col("days_since_enrolled_at") - F.avg("days_since_enrolled_at").over(w_student_id), 2).alias("days_since_enrolled_at_vs_student_id_avg"),
        F.lag("days_since_enrolled_at").over(w_all_by_days_since_enrolled_a).alias("prev_days_since_enrolled_at"),
    )
)

student_id_stats = (
    metrics
    .groupBy("student_id")
    .agg(F.avg("grade_status_len").alias("avg_grade_status_len"))
)

outliers = (
    metrics.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.grade_status_len") > F.col("g.avg_grade_status_len"))
    .select(
        "s.*",
        "g.avg_grade_status_len",
        F.round(F.col("s.grade_status_len") - F.col("g.avg_grade_status_len"), 2).alias("grade_status_len_minus_avg"),
    )
)

result = (
    outliers
    .filter(F.col("distinct_course_id_in_student_id") > 2)
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "days_since_enrolled_at",
        "grade_status_len",
        "term_grade_status_label",
        "enrolled_at_date",
        "days_since_prev_enrolled_at_date",
        "distinct_course_id_in_student_id",
        "enrolled_at_rank",
        "days_since_enrolled_at_vs_student_id_avg",
        "prev_days_since_enrolled_at",
        "grade_status_len_minus_avg",
        F.initcap(F.trim(F.col("term_grade_status_label"))).alias("term_grade_status_label_clean"),
        F.coalesce(F.col("enrolled_at_seq"), F.lit(0)).alias("enrolled_at_seq_filled"),
    )
    .orderBy("id", "prev_days_since_enrolled_at")
)
