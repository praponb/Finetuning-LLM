from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_n_term_2025t2 = Window.partitionBy("student_id").orderBy(F.col("n_term_2025t2").desc(), "student_id", "enrolled_at_day")
w_distinct_ids_student_id = Window.partitionBy("distinct_ids", "student_id")
w_all_by_cnt_prior_1 = Window.orderBy("cnt", "student_id", "enrolled_at_day").rowsBetween(-1, -1)
w_student_id_distinct_course_i = Window.partitionBy("student_id", "distinct_course_ids")

recent = (
    df_enrollments
    .filter((F.col("student_id") >= 14) & (F.col("term") == "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
        F.abs(F.col("score") - 34.2).alias("score_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

qualified = (
    recent
    .crossJoin(recent.agg(F.min("score_distance").alias("overall_min_score_distance")))
    .filter(F.col("score_distance") > F.col("overall_min_score_distance"))
    .drop("overall_min_score_distance")
)

totals = (
    qualified
    .groupBy("student_id", F.to_date("enrolled_at").alias("enrolled_at_day"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("term") != "2025T2", 1).otherwise(0)).alias("n_term_2025t2"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.count("*").alias("cnt"),
        F.sum("score").alias("sum_score_check"),
    )
    .filter(F.col("sum_score_check") > 34.2)
    .drop("sum_score_check")
)

metrics = (
    totals
    .withColumn("next_cnt", F.lead("cnt", 1, 0).over(w_student_id_by_n_term_2025t2))
    .withColumn("distinct_ids_student_id_sum_distinct_ids", F.sum("distinct_ids").over(w_distinct_ids_student_id))
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
    )
)

with_counts = (
    metrics.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

sequenced = (
    with_counts
    .withColumn("enrollment_count_sum_1_prior", F.sum("enrollment_count").over(w_all_by_cnt_prior_1))
    .withColumn("student_id_distinct_course_ids_min_n_term_2025t2", F.min("n_term_2025t2").over(w_student_id_distinct_course_i))
)

with_parent = (
    sequenced.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.major") == "Biz"), "inner")
    .select(
        "s.student_id",
        "s.enrolled_at_day",
        "s.n_term_2025t2",
        "s.term_list",
        "s.cnt",
        "s.next_cnt",
        "s.distinct_ids_student_id_sum_distinct_ids",
        "s.enrollment_count",
        "s.enrollment_count_2",
        "s.enrollment_count_sum_1_prior",
        "s.student_id_distinct_course_ids_min_n_term_2025t2",
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    with_parent
    .select(
        "student_id",
        "enrolled_at_day",
        "n_term_2025t2",
        "next_cnt",
        "distinct_ids_student_id_sum_distinct_ids",
        "enrollment_count",
        "enrollment_count_sum_1_prior",
        "student_id_distinct_course_ids_min_n_term_2025t2",
        F.year(F.col("enrolled_at_day")).alias("enrolled_at_day_year"),
        F.lower(F.trim(F.col("student_cohort"))).alias("student_cohort_clean"),
    )
    .orderBy("student_id")
)
