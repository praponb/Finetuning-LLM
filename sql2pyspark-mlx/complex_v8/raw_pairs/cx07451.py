from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term = Window.partitionBy("grade_status", "term")
w_student_id_by_enrolled_at_ti = Window.partitionBy("student_id").orderBy("enrolled_at")
w_student_id_by_enrolled_at_pr = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(-6, -1)
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_by_enrolled_at_ru = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_course_id_in_grade = Window.partitionBy("distinct_course_id_in_grade_status_term", F.date_trunc("month", "enrolled_at"))
w_distinct_course_id_in_grade_2 = Window.partitionBy("distinct_course_id_in_grade_status_term").orderBy(F.col("enrolled_at").desc())
w_cnt_distinct_course_id_in_gr = Window.partitionBy("cnt", "distinct_course_id_in_grade_status_term")
w_distinct_course_id_in_grade_3 = Window.partitionBy("distinct_course_id_in_grade_status_term", "course_dept").orderBy("cnt", "distinct_course_id_in_grade_status_term", "grade_status", "course_dept").rowsBetween(-1, Window.currentRow)
w_distinct_course_id_in_grade_4 = Window.partitionBy("distinct_course_id_in_grade_status_term", "course_dept").orderBy("cnt")

base_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")) & (F.col("score").between(5.6, 57.8)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

joined = (
    base_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select("s.*", "st.student_name", F.col("c.dept").alias("course_dept"))
)

metrics = (
    joined
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_filled",
        "student_name",
        "course_dept",
        F.count(F.lit(1)).over(w_grade_status_term).alias("rows_in_grade_status_term"),
        F.size(F.collect_set("course_id").over(w_grade_status_term)).alias("distinct_course_id_in_grade_status_term"),
        F.rank().over(w_student_id_by_enrolled_at_ti).alias("enrolled_at_rank"),
        F.round(F.col("score_filled") / F.nullif(F.sum("score_filled").over(w_grade_status_term), F.lit(0)), 4).alias("score_filled_share_of_grade_status_term"),
        F.sum("score_filled").over(w_grade_status_term).alias("grade_status_term_sum_score_filled"),
        F.round(F.avg("score_filled").over(w_student_id_by_enrolled_at_pr), 2).alias("score_filled_avg_6_prior"),
        F.max("score_filled").over(w_grade_status_term).alias("grade_status_term_max_score_filled"),
        F.ntile(4).over(w_student_id_by_enrolled_at).alias("enrolled_at_quartile"),
        F.last("score_filled", ignorenulls=True).over(w_student_id_by_enrolled_at_ru).alias("last_known_score_filled"),
    )
)

leaders = (
    metrics
    .filter((F.col("enrolled_at_rank") <= 2) & (F.col("enrolled_at_quartile") <= 6))
    .drop("enrolled_at_rank")
)

with_trends = (
    leaders
    .select(
        "*",
        F.min("last_known_score_filled").over(w_distinct_course_id_in_grade).alias("distinct_course_id_in_grade_status_term_enrolled"),
        F.dense_rank().over(w_distinct_course_id_in_grade_2).alias("enrolled_at_dense_rank"),
        F.round(F.cume_dist().over(w_distinct_course_id_in_grade_2), 3).alias("enrolled_at_cume_dist"),
    )
)

summary = (
    with_trends
    .filter(F.col("enrolled_at") < F.lit("2025-02-18 10:00:00").cast("timestamp"))
    .groupBy("distinct_course_id_in_grade_status_term", "grade_status", "course_dept")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("grade_status_term_max_score_filled") / F.nullif(F.sum("distinct_course_id_in_grade_status_term_enrolled"), F.lit(0)), 3).alias("grade_status_term_max_score_filled_to_distinct_c"),
    )
)

ranked = (
    summary
    .select(
        "*",
        F.round(F.col("cnt") / F.nullif(F.sum("cnt").over(w_cnt_distinct_course_id_in_gr), F.lit(0)), 4).alias("cnt_share_of_cnt_distinct_course_id_in_grade_sta"),
        F.round(F.avg("cnt").over(w_distinct_course_id_in_grade_3), 2).alias("cnt_avg_2"),
        F.round(F.percent_rank().over(w_distinct_course_id_in_grade_4), 3).alias("cnt_percent_rank"),
    )
)

result = (
    ranked
    .select(
        "distinct_course_id_in_grade_status_term",
        "grade_status",
        "course_dept",
        "cnt",
        "grade_status_term_max_score_filled_to_distinct_c",
        "cnt_share_of_cnt_distinct_course_id_in_grade_sta",
        "cnt_avg_2",
        "cnt_percent_rank",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("cnt").cast("string")).alias("grade_status_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
    .orderBy(F.col("grade_status").asc())
)
