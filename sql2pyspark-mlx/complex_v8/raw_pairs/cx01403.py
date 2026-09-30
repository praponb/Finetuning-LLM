from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_grade_status_len = Window.partitionBy("student_id", "grade_status_len")
w_course_id_by_grade_status_le = Window.partitionBy("course_id").orderBy("grade_status_len", "id")
w_student_is_international_gra = Window.partitionBy("student_is_international", "grade_status_len").orderBy(F.col("grade_status_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_len_student_is = Window.partitionBy("grade_status_len", "student_is_international").orderBy(F.col("enrolled_at").desc_nulls_first())
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_grade_status_len_student_id = Window.partitionBy("grade_status_len", "student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_is_international_gra_2 = Window.partitionBy("student_is_international", "grade_status_len").orderBy(F.col("grade_status_len").desc(), "id")
w_grade_status_len_student_is_2 = Window.partitionBy("grade_status_len", "student_is_international").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status = Window.partitionBy("grade_status")
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_running_score = Window.orderBy(F.col("running_score").desc(), "id")
w_enrolled_at_rank_term = Window.partitionBy("enrolled_at_rank", "term")
w_all_by_prev_score_ties = Window.orderBy("prev_score")
w_rn_by_score_change_moving_1 = Window.partitionBy("rn").orderBy("score_change", "id").rowsBetween(-1, Window.currentRow)
w_grade_status_len_decile_next = Window.partitionBy("grade_status_len_decile", "next_grade_status_len")
w_all_by_student_id_grade_stat = Window.orderBy("student_id_grade_status_len_min_grade_status_len", "id")
w_all_by_prev_score_running = Window.orderBy("prev_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_avg_next3_max_score_s = Window.orderBy(F.col("avg_next3_max_score_so_far").desc_nulls_first())
w_score_change_tier_student_is = Window.partitionBy("score_change_tier", "student_is_international")
w_all_by_avg_next3_max_score_s_2 = Window.orderBy("avg_next3_max_score_so_far", "enrolled_at_day", "score_change_tier", "student_is_international")

cleaned_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

joined_student = (
    cleaned_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_len",
        F.col("st.is_international").alias("student_is_international"),
    )
)

metrics = (
    joined_student
    .select(
        "*",
        F.count(F.lit(1)).over(w_student_id_grade_status_len).alias("rows_in_student_id_grade_status_len"),
        (F.col("score") - F.lag("score").over(w_course_id_by_grade_status_le)).alias("score_change"),
        F.sum("score").over(w_student_is_international_gra).alias("running_score"),
        F.round(F.percent_rank().over(w_grade_status_len_student_is), 3).alias("enrolled_at_percent_rank"),
        F.lag("score").over(w_all_by_enrolled_at).alias("prev_score"),
        F.max("score").over(w_grade_status_len_student_id).alias("max_score_so_far"),
        F.rank().over(w_all_by_enrolled_at).alias("enrolled_at_rank"),
        F.lead("grade_status_len").over(w_student_is_international_gra_2).alias("next_grade_status_len"),
        F.min("score").over(w_grade_status_len_student_is_2).alias("min_score_so_far"),
        F.size(F.collect_set("course_id").over(w_grade_status)).alias("distinct_course_id_in_grade_status"),
        F.min("grade_status_len").over(w_student_id_grade_status_len).alias("student_id_grade_status_len_min_grade_status_len"),
        F.first("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("first_known_enrolled_at"),
        F.row_number().over(w_course_id_by_grade_status_le).alias("rn"),
        F.ntile(10).over(w_course_id_by_grade_status_le).alias("grade_status_len_decile"),
    )
)

sequenced = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "student_is_international",
        "score_change",
        "enrolled_at_percent_rank",
        "max_score_so_far",
        "next_grade_status_len",
        "distinct_course_id_in_grade_status",
        "student_id_grade_status_len_min_grade_status_len",
        "first_known_enrolled_at",
        F.dense_rank().over(w_all_by_running_score).alias("running_score_dense_rank"),
        F.min("enrolled_at_percent_rank").over(w_enrolled_at_rank_term).alias("enrolled_at_rank_term_min_enrolled_at_percent_ra"),
        F.round(F.cume_dist().over(w_all_by_prev_score_ties), 3).alias("prev_score_cume_dist"),
        F.round(F.avg("next_grade_status_len").over(w_rn_by_score_change_moving_1), 2).alias("next_grade_status_len_avg_2"),
        F.max("min_score_so_far").over(w_grade_status_len_decile_next).alias("grade_status_len_decile_next_grade_status_len_ma"),
        F.lead("max_score_so_far", 3).over(w_all_by_student_id_grade_stat).alias("next3_max_score_so_far"),
        F.sum("max_score_so_far").over(w_all_by_prev_score_running).alias("running_max_score_so_far"),
        F.round(F.col("enrolled_at_percent_rank") / F.nullif(F.sum("enrolled_at_percent_rank").over(w_enrolled_at_rank_term), F.lit(0)), 4).alias("enrolled_at_percent_rank_share_of_enrolled_at_ra"),
    )
)

per_group = (
    sequenced
    .groupBy(
        F.to_date("enrolled_at").alias("enrolled_at_day"),
        F.when(F.col("score_change") >= 60.0, "A").when(F.col("score_change") >= -20.0, "B").otherwise("C").alias("score_change_tier"),
        "student_is_international",
    )
    .agg(
        F.round(F.avg("next3_max_score_so_far"), 2).alias("avg_next3_max_score_so_far"),
        F.max("score_change").alias("max_score_change"),
    )
)

metrics_2 = (
    per_group
    .select(
        "*",
        F.rank().over(w_all_by_avg_next3_max_score_s).alias("avg_next3_max_score_so_far_rank"),
        F.round(F.col("avg_next3_max_score_so_far") - F.avg("avg_next3_max_score_so_far").over(w_score_change_tier_student_is), 2).alias("avg_next3_max_score_so_far_vs_score_change_tier_"),
        F.max("avg_next3_max_score_so_far").over(w_score_change_tier_student_is).alias("score_change_tier_student_is_international_max_a"),
        F.lag("avg_next3_max_score_so_far", 2).over(w_all_by_avg_next3_max_score_s_2).alias("prev2_avg_next3_max_score_so_far"),
    )
)

result = (
    metrics_2
    .select(
        "enrolled_at_day",
        "score_change_tier",
        "student_is_international",
        "avg_next3_max_score_so_far",
        "avg_next3_max_score_so_far_vs_score_change_tier_",
        "score_change_tier_student_is_international_max_a",
        F.date_format(F.col("enrolled_at_day"), "yyyy-MM").alias("enrolled_at_day_label"),
    )
    .orderBy("enrolled_at_day", "score_change_tier", "student_is_international")
    .limit(5)
)
