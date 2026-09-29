from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_term_by_course_cr = Window.partitionBy("student_id", "term").orderBy(F.col("course_credits").desc(), "id").rowsBetween(-3, -1)
w_course_credits_student_id = Window.partitionBy("course_credits", "student_id")
w_student_id_term_by_course_cr_2 = Window.partitionBy("student_id", "term").orderBy(F.col("course_credits").desc())
w_student_id_term_by_course_cr_3 = Window.partitionBy("student_id", "term").orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_course_credits = Window.partitionBy("course_id").orderBy(F.col("course_credits").desc(), "id").rowsBetween(-6, Window.currentRow)
w_student_id_term_by_course_cr_4 = Window.partitionBy("student_id", "term").orderBy(F.col("course_credits").desc(), "id")
w_course_credits_max_3_prior_c = Window.partitionBy("course_credits_max_3_prior", "course_id").orderBy(F.col("course_credits_rank").desc())
w_course_id_term = Window.partitionBy("course_id", "term")
w_course_id_student_id_by_last = Window.partitionBy("course_id", "student_id").orderBy(F.col("last_course_credits").asc_nulls_last(), "id")
w_last_course_credits_course_c = Window.partitionBy("last_course_credits", "course_credits_seq")
w_all_by_stddev_last_course_cr = Window.orderBy(F.col("stddev_last_course_credits_course_credits_seq_ma").desc(), "course_credits_seq", "course_credits_avg_7_adj_tier", "last_course_credits")
w_all_by_stddev_last_course_cr_2 = Window.orderBy(F.col("stddev_last_course_credits_course_credits_seq_ma").desc())
w_distinct_ids = Window.partitionBy("distinct_ids")

filtered_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

enriched_course = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        F.col("c.credits").alias("course_credits"),
    )
)

metrics = (
    enriched_course
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        F.max("course_credits").over(w_student_id_term_by_course_cr).alias("course_credits_max_3_prior"),
        F.round(F.avg("course_credits").over(w_course_credits_student_id), 2).alias("course_credits_student_id_avg_course_credits"),
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_course_credits_student_id), 2).alias("course_credits_vs_course_credits_student_id_avg"),
        F.rank().over(w_student_id_term_by_course_cr_2).alias("course_credits_rank"),
        F.size(F.collect_set("course_id").over(w_course_credits_student_id)).alias("distinct_course_id_in_course_credits_student_id"),
        F.last("course_credits").over(w_student_id_term_by_course_cr_3).alias("last_course_credits"),
        F.round(F.avg("course_credits").over(w_course_id_by_course_credits), 2).alias("course_credits_avg_7"),
        F.row_number().over(w_student_id_term_by_course_cr_4).alias("course_credits_seq"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "course_credits_max_3_prior",
        "course_credits_vs_course_credits_student_id_avg",
        "course_credits_rank",
        "last_course_credits",
        "course_credits_avg_7",
        "course_credits_seq",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
        F.round(F.col("course_credits_avg_7") * 1.1, 2).alias("course_credits_avg_7_adj"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "student_id",
        "course_id",
        "course_credits_max_3_prior",
        "last_course_credits",
        "course_credits_avg_7",
        "course_credits_seq",
        "term_prefix",
        "course_credits_avg_7_adj",
        F.round(F.percent_rank().over(w_course_credits_max_3_prior_c), 3).alias("course_credits_rank_percent_rank"),
        F.round(F.col("course_credits_avg_7_adj") - F.avg("course_credits_avg_7_adj").over(w_course_id_term), 2).alias("course_credits_avg_7_adj_vs_course_id_term_avg"),
        F.row_number().over(w_course_id_student_id_by_last).alias("seq_no"),
        F.max("course_credits_max_3_prior").over(w_last_course_credits_course_c).alias("last_course_credits_course_credits_seq_max_cours"),
    )
)

summary = (
    with_windows
    .groupBy(
        "course_credits_seq",
        F.when(F.col("course_credits_avg_7_adj") >= 5.0, "A").when(F.col("course_credits_avg_7_adj") >= 2.8, "B").otherwise("C").alias("course_credits_avg_7_adj_tier"),
        "last_course_credits",
    )
    .agg(
        F.round(F.stddev("last_course_credits_course_credits_seq_max_cours"), 2).alias("stddev_last_course_credits_course_credits_seq_ma"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("last_course_credits"), 2).alias("stddev_last_course_credits"),
    )
)

with_trends = (
    summary
    .select(
        "*",
        F.ntile(10).over(w_all_by_stddev_last_course_cr).alias("stddev_last_course_credits_course_credits_seq_ma_2"),
        F.rank().over(w_all_by_stddev_last_course_cr_2).alias("stddev_last_course_credits_course_credits_seq_ma_3"),
        F.round(F.avg("stddev_last_course_credits").over(w_distinct_ids), 2).alias("distinct_ids_avg_stddev_last_course_credits"),
    )
)

result = (
    with_trends
    .select(
        "course_credits_seq",
        "course_credits_avg_7_adj_tier",
        "stddev_last_course_credits_course_credits_seq_ma",
        "distinct_ids",
        "stddev_last_course_credits",
        "stddev_last_course_credits_course_credits_seq_ma_2",
        "stddev_last_course_credits_course_credits_seq_ma_3",
        "distinct_ids_avg_stddev_last_course_credits",
        (F.col("n_rows") + F.col("last_course_credits")).alias("n_rows_plus_last_course_credits"),
    )
    .orderBy(F.col("stddev_last_course_credits").desc())
)
