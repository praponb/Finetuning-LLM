from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_term_by_enrolled_a = Window.partitionBy("course_id", "term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_by_enrolled_at_3 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc_nulls_first())
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_term_by_enrolled_a_2 = Window.partitionBy("course_id", "term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_rows = Window.orderBy("n_rows", "enrolled_at_dense_rank", "second_enrolled_at_2_year", "prev_enrolled_at_month")
w_all_by_n_rows_running = Window.orderBy("n_rows", "enrolled_at_dense_rank", "second_enrolled_at_2_year", "prev_enrolled_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
    )
)

windowed = (
    scoped_enrollments
    .select(
        "id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "enrolled_at_label",
        F.lag("enrolled_at").over(w_grade_status_by_enrolled_at).alias("prev_enrolled_at"),
        F.nth_value("enrolled_at", 2).over(w_all_by_enrolled_at_full).alias("second_enrolled_at"),
        F.last("enrolled_at").over(w_grade_status_by_enrolled_at_2).alias("last_enrolled_at"),
        F.nth_value("enrolled_at", 2).over(w_course_id_term_by_enrolled_a).alias("second_enrolled_at_2"),
        F.dense_rank().over(w_grade_status_by_enrolled_at_3).alias("enrolled_at_dense_rank"),
        F.lag("enrolled_at").over(w_all_by_enrolled_at).alias("prev_enrolled_at_2"),
        F.row_number().over(w_grade_status_by_enrolled_at).alias("rn"),
        F.first("enrolled_at", ignorenulls=True).over(w_course_id_term_by_enrolled_a_2).alias("first_known_enrolled_at"),
        F.rank().over(w_all_by_enrolled_at).alias("enrolled_at_rank"),
    )
)

labeled = (
    windowed
    .filter(F.col("enrolled_at_label").isNotNull())
    .select(
        "*",
        (F.col("enrolled_at_rank") - F.col("rn")).alias("enrolled_at_rank_minus_rn"),
        F.when(F.col("enrolled_at_dense_rank") >= 6, "large").when(F.col("enrolled_at_dense_rank") >= 5, "medium").otherwise("small").alias("enrolled_at_dense_rank_tier"),
        F.date_format(F.col("last_enrolled_at"), "MMM yyyy").alias("last_enrolled_at_label"),
    )
)

stats = (
    labeled
    .groupBy(
        "enrolled_at_dense_rank",
        F.year("second_enrolled_at_2").alias("second_enrolled_at_2_year"),
        F.date_trunc("month", "prev_enrolled_at").alias("prev_enrolled_at_month"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.round(F.stddev("rn"), 2).alias("stddev_rn"),
    )
)

metrics = (
    stats
    .select(
        "*",
        (F.col("enrolled_at_dense_rank") - F.lag("enrolled_at_dense_rank").over(w_all_by_n_rows)).alias("enrolled_at_dense_rank_change"),
        F.sum("n_rows").over(w_all_by_n_rows_running).alias("running_n_rows"),
    )
)

result = (
    metrics
    .select(
        "enrolled_at_dense_rank",
        "second_enrolled_at_2_year",
        "prev_enrolled_at_month",
        "grade_status_list",
        "stddev_rn",
        "enrolled_at_dense_rank_change",
        "running_n_rows",
        F.round(F.col("stddev_rn") * 1.07, 2).alias("stddev_rn_adj"),
        F.date_format(F.col("prev_enrolled_at_month"), "yyyy-MM-dd HH:00").alias("prev_enrolled_at_month_label"),
    )
    .orderBy("prev_enrolled_at_month")
)
