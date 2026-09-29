from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_major_by_enrolled_at = Window.partitionBy("student_major").orderBy(F.col("enrolled_at_hour").desc(), "id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_student_major_enrolled_at_mo = Window.partitionBy("student_major", F.date_trunc("month", "enrolled_at"))
w_student_major_enrolled_at_ho = Window.partitionBy("student_major", "enrolled_at_hour")
w_student_major_course_id_by_e = Window.partitionBy("student_major", "course_id").orderBy("enrolled_at", "id")
w_distinct_grade_status_in_stu = Window.partitionBy("distinct_grade_status_in_student_major_enrolled_", "rn").orderBy("enrolled_at_hour", "id")
w_student_major = Window.partitionBy("student_major")
w_next_enrolled_at_hour_enroll = Window.partitionBy("next_enrolled_at_hour", "enrolled_at_hour").orderBy("enrolled_at_hour", "id")
w_distinct_student_major_group = Window.partitionBy("distinct_student_major_group_in_student_major", "next_enrolled_at_hour")
w_all_by_n_grade_status_in_wit = Window.orderBy("n_grade_status_in_withdrawn_pa", "next_enrolled_at_hour", "distinct_student_major_group_in_student_major", "distinct_grade_status_in_student_major_enrolled_")

valid_enrollments = (
    df_enrollments
    .filter(F.col("student_id") <= 5)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

combined_details_student = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_hour",
        F.col("st.major").alias("student_major"),
        "st.student_name",
    )
)

with_trends = (
    combined_details_student
    .select(
        "*",
        F.rank().over(w_student_major_by_enrolled_at).alias("enrolled_at_hour_rank"),
        F.rank().over(w_all_by_enrolled_at).alias("enrolled_at_rank"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_major_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.avg("enrolled_at_hour").over(w_student_major_enrolled_at_mo), 2).alias("student_major_enrolled_at_month_avg_enrolled_at_"),
        F.lead("enrolled_at_hour").over(w_all_by_enrolled_at).alias("next_enrolled_at_hour"),
        F.count(F.lit(1)).over(w_student_major_enrolled_at_mo).alias("rows_in_student_major_enrolled_at_month"),
        F.size(F.collect_set("grade_status").over(w_student_major_enrolled_at_ho)).alias("distinct_grade_status_in_student_major_enrolled_"),
        F.row_number().over(w_student_major_course_id_by_e).alias("rn"),
    )
)

labeled = (
    with_trends
    .filter(F.lower(F.col("student_name")).like("%lo%"))
    .withColumn("student_major_group", F.when(F.col("student_major").isin("Art", "CS"), "group_a").when(F.col("student_major") == "Math", "group_b").otherwise("other"))
)

ranked = (
    labeled
    .select(
        "*",
        F.lag("rn").over(w_distinct_grade_status_in_stu).alias("prev_rn"),
        F.count(F.lit(1)).over(w_student_major).alias("rows_in_student_major"),
        F.rank().over(w_next_enrolled_at_hour_enroll).alias("enrolled_at_hour_rank_2"),
        F.size(F.collect_set("student_major_group").over(w_student_major)).alias("distinct_student_major_group_in_student_major"),
        (F.col("enrolled_at_hour") - F.lag("enrolled_at_hour").over(w_distinct_grade_status_in_stu)).alias("enrolled_at_hour_change"),
        (F.col("rn") - F.lag("rn").over(w_distinct_grade_status_in_stu)).alias("rn_change"),
    )
)

rollup_base = (
    ranked
    .groupBy(
        "next_enrolled_at_hour",
        "distinct_student_major_group_in_student_major",
        "distinct_grade_status_in_student_major_enrolled_",
    )
    .agg(
        F.count(F.when(F.col("grade_status").isin("WITHDRAWN", "PASS", "INCOMPLETE"), 1)).alias("n_grade_status_in_withdrawn_pa"),
        F.min("rows_in_student_major").alias("min_rows_in_student_major"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.lower(F.col("student_name")).like("%a %"), 1).otherwise(0)).alias("n_lower_student_name_like_a"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.round(F.sum("rn_change") / F.nullif(F.sum("enrolled_at_rank"), F.lit(0)), 3).alias("rn_change_to_enrolled_at_rank_ratio"),
    )
)

with_trends_2 = (
    rollup_base
    .select(
        "*",
        F.round(F.col("next_enrolled_at_hour") - F.avg("next_enrolled_at_hour").over(w_distinct_student_major_group), 2).alias("next_enrolled_at_hour_vs_distinct_student_major_"),
        F.row_number().over(w_all_by_n_grade_status_in_wit).alias("rn"),
        F.round(F.percent_rank().over(w_all_by_n_grade_status_in_wit), 3).alias("n_grade_status_in_withdrawn_pa_percent_rank"),
    )
)

result = (
    with_trends_2
    .select(
        "next_enrolled_at_hour",
        "distinct_student_major_group_in_student_major",
        "distinct_grade_status_in_student_major_enrolled_",
        "n_grade_status_in_withdrawn_pa",
        "cnt",
        "n_lower_student_name_like_a",
        "first_enrolled_at",
        "rn_change_to_enrolled_at_rank_ratio",
        "next_enrolled_at_hour_vs_distinct_student_major_",
        "rn",
        "n_grade_status_in_withdrawn_pa_percent_rank",
        F.when(F.col("next_enrolled_at_hour") >= 14, "HIGH").when(F.col("next_enrolled_at_hour") >= 9, "MEDIUM").otherwise("LOW").alias("next_enrolled_at_hour_tier"),
    )
    .orderBy(
        F.col("n_grade_status_in_withdrawn_pa_percent_rank").desc(),
        F.col("next_enrolled_at_hour_vs_distinct_student_major_").desc(),
    )
)
