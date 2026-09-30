from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").asc_nulls_last())
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_course_id_by_enro = Window.partitionBy("student_id", "course_id").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at_full_2 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_ties_2 = Window.orderBy(F.col("enrolled_at").desc_nulls_first())
w_all_by_enrolled_at_2 = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at_3 = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_all_by_enrolled_at_ties_3 = Window.orderBy(F.col("enrolled_at").desc())
w_all_by_prev2_enrolled_at = Window.orderBy("prev2_enrolled_at", "id")
w_enrolled_at_quintile_first_e = Window.partitionBy("enrolled_at_quintile", F.date_trunc("month", "first_enrolled_at_2")).orderBy(F.col("enrolled_at_percent_rank").desc(), "id")
w_grade_status_group_last_enro = Window.partitionBy("grade_status_group", F.date_trunc("month", "last_enrolled_at")).orderBy(F.col("first_enrolled_at_2").desc(), "id")

prepared_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") == "FAIL") & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("FAIL", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

ranked = (
    prepared_enrollments
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
        F.lag("enrolled_at").over(w_all_by_enrolled_at).alias("prev_enrolled_at"),
        F.last("enrolled_at").over(w_all_by_enrolled_at_full).alias("last_enrolled_at"),
        F.ntile(5).over(w_all_by_enrolled_at).alias("enrolled_at_quintile"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_id_course_id_by_enro))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.first("enrolled_at").over(w_all_by_enrolled_at_full_2).alias("first_enrolled_at"),
        F.first("enrolled_at").over(w_all_by_enrolled_at_full).alias("first_enrolled_at_2"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties_2), 3).alias("enrolled_at_cume_dist_2"),
        F.lag("enrolled_at", 2).over(w_all_by_enrolled_at).alias("prev2_enrolled_at"),
        F.row_number().over(w_all_by_enrolled_at_2).alias("seq_no"),
        F.rank().over(w_all_by_enrolled_at_3).alias("enrolled_at_rank"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties_3), 3).alias("enrolled_at_percent_rank"),
    )
)

leaders = (
    ranked
    .filter((F.col("seq_no") == 1) & (F.hour(F.col("prev_enrolled_at")).between(9, 13)))
    .drop("seq_no")
)

ranked_2 = (
    leaders
    .select(
        "*",
        F.dense_rank().over(w_all_by_prev2_enrolled_at).alias("prev2_enrolled_at_dense_rank"),
        F.round(F.percent_rank().over(w_enrolled_at_quintile_first_e), 3).alias("enrolled_at_percent_rank_percent_rank"),
        F.rank().over(w_grade_status_group_last_enro).alias("first_enrolled_at_2_rank"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "grade_status_group",
        "enrolled_at_cume_dist",
        "prev_enrolled_at",
        "last_enrolled_at",
        "enrolled_at_quintile",
        "minutes_since_prev_enrolled_at",
        "first_enrolled_at",
        "first_enrolled_at_2",
        "enrolled_at_rank",
        "enrolled_at_percent_rank",
        "enrolled_at_percent_rank_percent_rank",
        "first_enrolled_at_2_rank",
        F.coalesce(F.col("enrolled_at_percent_rank"), F.lit(0.0)).alias("enrolled_at_percent_rank_filled"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
    .orderBy("enrolled_at_cume_dist")
)
