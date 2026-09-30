from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_dept_student_id_by_en = Window.partitionBy("course_dept", "student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_score_tier_by_enro = Window.partitionBy("course_id", "score_tier").orderBy(F.col("enrolled_at").desc(), "id")
w_course_dept_student_id_by_en_2 = Window.partitionBy("course_dept", "student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_score_tier_by_enrolled_at = Window.partitionBy("score_tier").orderBy("enrolled_at", "id")
w_student_id_score_tier_by_enr = Window.partitionBy("student_id", "score_tier").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_enrolled_at_fu = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_score_tier_by_enro_2 = Window.partitionBy("course_id", "score_tier").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_enrolled_at_2_mon = Window.partitionBy(F.date_trunc("month", "last_known_enrolled_at_2"))
w_enrolled_at_decile_student_i = Window.partitionBy("enrolled_at_decile", "student_id").orderBy(F.col("last_known_enrolled_at_2").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_decile_student_i_2 = Window.partitionBy("enrolled_at_decile", "student_id").orderBy(F.col("last_known_enrolled_at_2").asc_nulls_last(), "id")
w_all_by_minutes_since_prev_en = Window.orderBy(F.col("minutes_since_prev_enrolled_at").desc_nulls_first(), "id")

valid = (
    df_enrollments
    .filter(F.col("score").between(16.3, 80.2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
        F.when(F.col("score") >= 56.1, "HIGH").when(F.col("score") >= 43.5, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

joined = (
    valid.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") >= 2), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_tier",
        F.col("c.dept").alias("course_dept"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.first("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("first_known_enrolled_at"),
        F.nth_value("enrolled_at", 2).over(w_course_dept_student_id_by_en).alias("second_enrolled_at"),
        F.lag("enrolled_at").over(w_course_id_score_tier_by_enro).alias("prev_enrolled_at"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_dept_student_id_by_en_2))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.ntile(10).over(w_course_id_score_tier_by_enro).alias("enrolled_at_decile"),
        F.row_number().over(w_score_tier_by_enrolled_at).alias("enrolled_at_seq"),
        F.last("enrolled_at", ignorenulls=True).over(w_student_id_score_tier_by_enr).alias("last_known_enrolled_at"),
        F.last("enrolled_at").over(w_student_id_by_enrolled_at_fu).alias("last_enrolled_at"),
        F.last("enrolled_at", ignorenulls=True).over(w_course_id_score_tier_by_enro_2).alias("last_known_enrolled_at_2"),
        F.lag("enrolled_at", 3).over(w_score_tier_by_enrolled_at).alias("prev3_enrolled_at"),
        F.lag("enrolled_at").over(w_course_dept_student_id_by_en_2).alias("prev_enrolled_at_2"),
    )
)

leaders = (
    metrics
    .filter(F.col("enrolled_at_seq") <= 3)
    .drop("enrolled_at_seq")
)

sequenced = (
    leaders
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "course_dept",
        "first_known_enrolled_at",
        "second_enrolled_at",
        "prev_enrolled_at",
        "enrolled_at_decile",
        "last_known_enrolled_at_2",
        "prev_enrolled_at_2",
        F.max("enrolled_at_decile").over(w_last_known_enrolled_at_2_mon).alias("last_known_enrolled_at_2_month_max_enrolled_at_d"),
        F.size(F.collect_set("course_id").over(w_last_known_enrolled_at_2_mon)).alias("distinct_course_id_in_last_known_enrolled_at_2_m"),
        F.count(F.lit(1)).over(w_enrolled_at_decile_student_i).alias("running_count"),
        F.round(F.col("enrolled_at_decile") - F.avg("enrolled_at_decile").over(w_last_known_enrolled_at_2_mon), 2).alias("enrolled_at_decile_vs_last_known_enrolled_at_2_m"),
        F.rank().over(w_enrolled_at_decile_student_i_2).alias("last_known_enrolled_at_2_rank"),
        F.ntile(4).over(w_all_by_minutes_since_prev_en).alias("minutes_since_prev_enrolled_at_quartile"),
        F.round(F.col("enrolled_at_decile") / F.nullif(F.sum("enrolled_at_decile").over(w_last_known_enrolled_at_2_mon), F.lit(0)), 4).alias("enrolled_at_decile_share_of_last_known_enrolled_"),
    )
)

grouped = (
    sequenced
    .filter(F.col("enrolled_at_decile_vs_last_known_enrolled_at_2_m") > -5.0)
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"))
    .agg(
        F.count(F.when(F.col("course_dept") == "Math", 1)).alias("n_course_dept_math"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.count("*").alias("cnt"),
        F.sum("minutes_since_prev_enrolled_at_quartile").alias("total_minutes_since_prev_enrolled_at_quartile"),
        F.round(F.sum("distinct_course_id_in_last_known_enrolled_at_2_m") / F.nullif(F.sum("enrolled_at_decile_vs_last_known_enrolled_at_2_m"), F.lit(0)), 3).alias("distinct_course_id_in_last_known_enrolled_at_2_m_2"),
        F.max("prev_enrolled_at").alias("last_prev_enrolled_at"),
    )
)

result = (
    grouped
    .select(
        "enrolled_at_month",
        "n_course_dept_math",
        "grade_status_list",
        "total_minutes_since_prev_enrolled_at_quartile",
        "distinct_course_id_in_last_known_enrolled_at_2_m_2",
        "last_prev_enrolled_at",
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
        F.abs(F.col("cnt") - 8).alias("cnt_distance"),
    )
    .orderBy(
        F.col("distinct_course_id_in_last_known_enrolled_at_2_m_2").asc_nulls_last(),
        F.col("enrolled_at_month").asc(),
    )
)
