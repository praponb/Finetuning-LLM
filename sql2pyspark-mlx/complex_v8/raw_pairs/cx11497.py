from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_id_by_s = Window.partitionBy("grade_status", "student_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month_term = Window.partitionBy(F.month("enrolled_at"), "term")
w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_student_id_by_s_2 = Window.partitionBy("grade_status", "student_id").orderBy(F.col("score").desc(), "id")
w_course_id_grade_status_by_en_2 = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id")
w_student_id_grade_status_by_s = Window.partitionBy("student_id", "grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(-2, Window.currentRow)
w_n_grade_status_in_withdrawn = Window.partitionBy("n_grade_status_in_withdrawn_in", "distinct_grade_status_group_in_enrolled_at_month").orderBy(F.col("max_score_avg_3_filled").asc_nulls_last(), "distinct_grade_status_group_in_enrolled_at_month")
w_n_grade_status_in_withdrawn_2 = Window.partitionBy("n_grade_status_in_withdrawn_in", "distinct_grade_status_group_in_enrolled_at_month").orderBy(F.col("max_score_avg_3_filled").asc_nulls_last(), "distinct_grade_status_group_in_enrolled_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_grade_status_group = Window.partitionBy("distinct_grade_status_group_in_enrolled_at_month").orderBy(F.col("max_score_avg_3_filled").asc_nulls_last(), "distinct_grade_status_group_in_enrolled_at_month")

scoped_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("student_id") <= 8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "PASS", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

combined_details_student = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select("s.*", F.col("st.major").alias("student_major"))
)

sequenced = (
    combined_details_student
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_group",
        "student_major",
        F.first("enrolled_at").over(w_grade_status_student_id_by_s).alias("first_enrolled_at"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_month_term), F.lit(0)), 4).alias("score_share_of_enrolled_at_month_term"),
        F.first("enrolled_at", ignorenulls=True).over(w_course_id_grade_status_by_en).alias("first_known_enrolled_at"),
        F.round(F.avg("score").over(w_enrolled_at_month_term), 2).alias("enrolled_at_month_term_avg_score"),
        F.size(F.collect_set("grade_status_group").over(w_enrolled_at_month_term)).alias("distinct_grade_status_group_in_enrolled_at_month"),
        F.lag("score", 2).over(w_grade_status_student_id_by_s_2).alias("prev2_score"),
        F.row_number().over(w_grade_status_student_id_by_s_2).alias("score_seq"),
        F.round(F.percent_rank().over(w_grade_status_student_id_by_s_2), 3).alias("score_percent_rank"),
        F.size(F.collect_set("grade_status").over(w_enrolled_at_month_term)).alias("distinct_grade_status_in_enrolled_at_month_term"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_id_grade_status_by_en_2))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.avg("score").over(w_student_id_grade_status_by_s), 2).alias("score_avg_3"),
        F.round(F.cume_dist().over(w_grade_status_student_id_by_s_2), 3).alias("score_cume_dist"),
        F.lag("score", 2, 0).over(w_grade_status_student_id_by_s_2).alias("prev2_score_2"),
    )
)

enriched = (
    sequenced
    .filter(F.col("student_id") <= 8)
    .select(
        "*",
        F.abs(F.col("score") - 50.9).alias("score_distance"),
        (F.col("score_avg_3") - F.col("prev2_score_2")).alias("score_avg_3_minus_prev2_score_2"),
        F.coalesce(F.col("score_avg_3"), F.lit(0.0)).alias("score_avg_3_filled"),
    )
)

rollup_base = (
    enriched
    .groupBy("distinct_grade_status_group_in_enrolled_at_month")
    .agg(
        F.sum(F.when(F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE"), 1).otherwise(0)).alias("n_grade_status_in_withdrawn_in"),
        F.max("score_avg_3_filled").alias("max_score_avg_3_filled"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
    )
)

ranked = (
    rollup_base
    .select(
        "*",
        F.round(F.percent_rank().over(w_n_grade_status_in_withdrawn), 3).alias("max_score_avg_3_filled_percent_rank"),
        F.last("max_score_avg_3_filled").over(w_n_grade_status_in_withdrawn_2).alias("last_max_score_avg_3_filled"),
        F.round(F.cume_dist().over(w_n_grade_status_in_withdrawn), 3).alias("max_score_avg_3_filled_cume_dist"),
        F.rank().over(w_distinct_grade_status_group).alias("max_score_avg_3_filled_rank"),
    )
)

result = (
    ranked
    .filter(F.col("distinct_grade_status_group_in_enrolled_at_month") <= 8)
    .select(
        "distinct_grade_status_group_in_enrolled_at_month",
        "n_grade_status_in_withdrawn_in",
        "max_score_avg_3_filled",
        "term_list",
        "max_score_avg_3_filled_percent_rank",
        "last_max_score_avg_3_filled",
        "max_score_avg_3_filled_cume_dist",
        "max_score_avg_3_filled_rank",
        F.abs(F.col("max_score_avg_3_filled") - 60.0).alias("max_score_avg_3_filled_distance"),
    )
    .orderBy(F.col("n_grade_status_in_withdrawn_in").asc_nulls_last())
)
