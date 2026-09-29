from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_term_by_enrolled_a = Window.partitionBy("course_id", "term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id = Window.partitionBy("student_id")
w_term = Window.partitionBy("term")
w_term_enrolled_at_dayofweek_b = Window.partitionBy("term", "enrolled_at_dayofweek").orderBy("enrolled_at_dayofweek", "id").rowsBetween(-6, -1)
w_enrolled_at_month_grade_stat = Window.partitionBy(F.month("enrolled_at"), "grade_status").orderBy(F.col("enrolled_at_dayofweek").desc_nulls_first(), "id")
w_course_id_term_by_enrolled_a_2 = Window.partitionBy("course_id", "term").orderBy("enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_term_course_id = Window.partitionBy("term", "course_id")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month_grade_stat_2 = Window.partitionBy(F.month("enrolled_at"), "grade_status").orderBy(F.col("enrolled_at_dayofweek").desc_nulls_first())
w_all_by_enrolled_at_dayofweek = Window.orderBy("enrolled_at_dayofweek", "id").rowsBetween(-1, -1)
w_seq_no_by_total_max_enrolled = Window.partitionBy("seq_no").orderBy(F.col("total_max_enrolled_at_dayofweek_so_far").asc_nulls_last(), "seq_no")

recent_enrollments = (
    df_enrollments
    .filter((F.col("term").isin("2025T2", "2025T1")) & (F.col("course_id") >= 4))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

metrics = (
    recent_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "enrolled_at_dayofweek",
        "grade_status_clean",
        F.max("enrolled_at_dayofweek").over(w_course_id_term_by_enrolled_a).alias("max_enrolled_at_dayofweek_so_far"),
        F.sum("enrolled_at_dayofweek").over(w_student_id).alias("student_id_sum_enrolled_at_dayofweek"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.sum("enrolled_at_dayofweek").over(w_term_enrolled_at_dayofweek_b).alias("enrolled_at_dayofweek_sum_6_prior"),
        F.row_number().over(w_enrolled_at_month_grade_stat).alias("seq_no"),
        F.max("enrolled_at_dayofweek").over(w_term).alias("term_max_enrolled_at_dayofweek"),
        F.max("enrolled_at_dayofweek").over(w_course_id_term_by_enrolled_a_2).alias("enrolled_at_dayofweek_max_7"),
        F.size(F.collect_set("grade_status").over(w_term_course_id)).alias("distinct_grade_status_in_term_course_id"),
        F.nth_value("enrolled_at", 2).over(w_all_by_enrolled_at_full).alias("second_enrolled_at"),
        F.round(F.percent_rank().over(w_enrolled_at_month_grade_stat_2), 3).alias("enrolled_at_dayofweek_percent_rank"),
        F.sum("enrolled_at_dayofweek").over(w_all_by_enrolled_at_dayofweek).alias("enrolled_at_dayofweek_sum_1_prior"),
    )
)

annotated = (
    metrics
    .filter(F.col("rows_in_term").between(3, 18))
    .withColumn("term_slug", F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")))
)

per_group = (
    annotated
    .groupBy("seq_no")
    .agg(
        F.sum("term_max_enrolled_at_dayofweek").alias("total_term_max_enrolled_at_dayofweek"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.sum("max_enrolled_at_dayofweek_so_far").alias("total_max_enrolled_at_dayofweek_so_far"),
    )
)

metrics_2 = (
    per_group
    .select(
        "seq_no",
        "total_term_max_enrolled_at_dayofweek",
        "term_list",
        "total_max_enrolled_at_dayofweek_so_far",
        (F.col("seq_no") - F.lag("seq_no").over(w_seq_no_by_total_max_enrolled)).alias("seq_no_change"),
        F.ntile(3).over(w_seq_no_by_total_max_enrolled).alias("total_max_enrolled_at_dayofweek_so_far_tercile"),
    )
)

result = (
    metrics_2
    .filter(F.col("seq_no") >= 2)
    .select(
        "seq_no",
        "total_term_max_enrolled_at_dayofweek",
        "term_list",
        "total_max_enrolled_at_dayofweek_so_far",
        "seq_no_change",
        "total_max_enrolled_at_dayofweek_so_far_tercile",
        F.abs(F.col("seq_no") - 7).alias("seq_no_distance"),
        (F.floor(F.col("seq_no_change") / 5) * 5).alias("seq_no_change_bucket"),
    )
    .orderBy(F.col("seq_no_distance").asc_nulls_last(), F.col("seq_no").asc())
)
