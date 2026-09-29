from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_distinct_course_i = Window.partitionBy("student_id", "distinct_course_ids").orderBy("n_term_in_2024t2_2024t1_2025t1", "course_id", "student_id", "term").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_n_term_in_2024t = Window.partitionBy("course_id").orderBy("n_term_in_2024t2_2024t1_2025t1", "course_id", "student_id", "term")
w_course_id_by_n_term_in_2024t_2 = Window.partitionBy("course_id").orderBy("n_term_in_2024t2_2024t1_2025t1", "course_id", "student_id", "term").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id = Window.partitionBy("course_id")
w_n_term_in_2024t2_2024t1_2025 = Window.partitionBy("n_term_in_2024t2_2024t1_2025t1", "term").orderBy("n_term_in_2024t2_2024t1_2025t1", "course_id", "student_id", "term").rowsBetween(-6, -1)
w_n_term_in_2024t2_2024t1_2025_2 = Window.partitionBy("n_term_in_2024t2_2024t1_2025t1", "distinct_term_in_course_id").orderBy(F.col("distinct_term_in_course_id").desc(), "course_id", "student_id", "term")
w_n_term_in_2024t2_2024t1_2025_3 = Window.partitionBy("n_term_in_2024t2_2024t1_2025t1").orderBy("n_term_in_2024t2_2024t1_2025t1_decile", "course_id", "student_id", "term").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_term_in_2024t2_2024t1_2025_4 = Window.partitionBy("n_term_in_2024t2_2024t1_2025t1").orderBy("n_term_in_2024t2_2024t1_2025t1_decile", "course_id", "student_id", "term").rowsBetween(-1, Window.currentRow)
w_n_term_in_2024t2_2024t1_2025_5 = Window.partitionBy("n_term_in_2024t2_2024t1_2025t1").orderBy("n_term_in_2024t2_2024t1_2025t1_decile")
w_n_term_in_2024t2_2024t1_2025_6 = Window.partitionBy("n_term_in_2024t2_2024t1_2025t1", "distinct_term_in_course_id").orderBy(F.col("distinct_term_in_course_id").desc(), "course_id", "student_id", "term").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_term_in_2024t2_2024t1_2025_7 = Window.partitionBy("n_term_in_2024t2_2024t1_2025t1", "distinct_term_in_course_id").orderBy(F.col("distinct_term_in_course_id").desc(), "course_id", "student_id", "term").rowsBetween(-2, -1)

prepared = (
    df_enrollments
    .filter(F.col("student_id") >= 4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
)

grouped = (
    prepared
    .filter(F.col("course_id") >= 4)
    .groupBy("course_id", "student_id", "term")
    .agg(
        F.sum("score").alias("total_score"),
        F.count(F.when(F.col("term").isin("2024T2", "2024T1", "2025T1"), 1)).alias("n_term_in_2024t2_2024t1_2025t1"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
    )
)

ranked = (
    grouped
    .select(
        "course_id",
        "student_id",
        "term",
        "total_score",
        "n_term_in_2024t2_2024t1_2025t1",
        "term_list",
        "distinct_course_ids",
        F.last("total_score").over(w_student_id_distinct_course_i).alias("last_total_score"),
        F.ntile(10).over(w_course_id_by_n_term_in_2024t).alias("n_term_in_2024t2_2024t1_2025t1_decile"),
        F.last("total_score", ignorenulls=True).over(w_course_id_by_n_term_in_2024t_2).alias("last_known_total_score"),
        F.size(F.collect_set("term").over(w_course_id)).alias("distinct_term_in_course_id"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.max("distinct_course_ids").over(w_n_term_in_2024t2_2024t1_2025).alias("distinct_course_ids_max_6_prior"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        (F.col("distinct_term_in_course_id") - F.lag("distinct_term_in_course_id").over(w_n_term_in_2024t2_2024t1_2025_2)).alias("distinct_term_in_course_id_change"),
        F.first("last_known_total_score").over(w_n_term_in_2024t2_2024t1_2025_3).alias("first_last_known_total_score"),
        F.round(F.avg("last_total_score").over(w_n_term_in_2024t2_2024t1_2025_4), 2).alias("last_total_score_avg_2"),
        F.round(F.cume_dist().over(w_n_term_in_2024t2_2024t1_2025_5), 3).alias("n_term_in_2024t2_2024t1_2025t1_decile_cume_dist"),
        F.first("last_total_score", ignorenulls=True).over(w_n_term_in_2024t2_2024t1_2025_6).alias("first_known_last_total_score"),
        F.max("distinct_course_ids").over(w_n_term_in_2024t2_2024t1_2025_7).alias("distinct_course_ids_max_2_prior"),
    )
)

with_details_course = (
    metrics.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.course_id",
        "s.student_id",
        "s.term",
        "s.n_term_in_2024t2_2024t1_2025t1",
        "s.term_list",
        "s.last_total_score",
        "s.last_known_total_score",
        "s.distinct_term_in_course_id",
        "s.rows_in_course_id",
        "s.distinct_course_ids_max_6_prior",
        "s.distinct_term_in_course_id_change",
        "s.last_total_score_avg_2",
        "s.n_term_in_2024t2_2024t1_2025t1_decile_cume_dist",
        "s.first_known_last_total_score",
        "s.distinct_course_ids_max_2_prior",
        "c.course_code",
        F.col("c.level").alias("course_level"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
    )
)

annotated = (
    with_details_course
    .filter(F.col("student_major") != "Art")
    .withColumn("student_major_ref", F.concat(F.col("student_major"), F.lit("#"), F.col("student_id").cast("string")))
)

result = (
    annotated
    .groupingSets(
        [["student_id", "term", "distinct_term_in_course_id"], ["student_id"], ["term"]],
        "student_id", "term", "distinct_term_in_course_id",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("term").isNotNull(), F.col("n_term_in_2024t2_2024t1_2025t1_decile_cume_dist"))).alias("n_term_in_2024t2_2024t1_2025t1_decile_cume_dist_"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
    )
)
