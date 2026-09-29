from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_term_by_enrolled_at_ties = Window.partitionBy("term").orderBy("enrolled_at")
w_course_id_by_enrolled_at_ful = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_all_by_enrolled_at_2 = Window.orderBy("enrolled_at", "id")
w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_enrollments
    .filter((F.col("student_id") <= 3) & (F.col("term").isin("2024T1", "2025T2")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
        F.ntile(10).over(w_all_by_enrolled_at).alias("enrolled_at_decile"),
        F.dense_rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
        F.dense_rank().over(w_term_by_enrolled_at_ties).alias("enrolled_at_dense_rank_2"),
        F.lead("enrolled_at").over(w_all_by_enrolled_at).alias("next_enrolled_at"),
        F.last("enrolled_at").over(w_course_id_by_enrolled_at_ful).alias("last_enrolled_at"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.percent_rank().over(w_course_id_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
        F.dense_rank().over(w_all_by_enrolled_at_2).alias("enrolled_at_dense_rank_3"),
        F.ntile(4).over(w_term_by_enrolled_at).alias("enrolled_at_quartile"),
        F.first("enrolled_at").over(w_all_by_enrolled_at_full).alias("first_enrolled_at"),
        F.lag("enrolled_at").over(w_term_by_enrolled_at).alias("prev_enrolled_at"),
        F.lag("enrolled_at", 2).over(w_all_by_enrolled_at_2).alias("prev2_enrolled_at"),
    )
)

leaders = (
    windowed
    .filter((F.col("enrolled_at_dense_rank_3") == 1) & (F.col("enrolled_at_percent_rank") > 0.5))
)

result = (
    leaders
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrolled_at",
        "enrolled_at_cume_dist",
        "enrolled_at_decile",
        "enrolled_at_dense_rank_2",
        "next_enrolled_at",
        "last_enrolled_at",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_percent_rank",
        "enrolled_at_dense_rank_3",
        "prev_enrolled_at",
        F.abs(F.col("enrolled_at_dense_rank_2") - 6).alias("enrolled_at_dense_rank_2_distance"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
    .orderBy(F.col("enrolled_at_label").desc(), "id")
    .limit(5)
)
