from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_term_by_enrolled_a = Window.partitionBy("course_id", "term").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_2 = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").asc_nulls_last())
w_all_by_enrolled_at_3 = Window.orderBy("enrolled_at", "id")

base_enrollments = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(8, 14))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

metrics = (
    base_enrollments
    .select(
        "id",
        "term",
        "enrolled_at",
        "term_slug",
        F.lag("enrolled_at", 2).over(w_all_by_enrolled_at).alias("prev2_enrolled_at"),
        F.nth_value("enrolled_at", 2).over(w_course_id_term_by_enrolled_a).alias("second_enrolled_at"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.last("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("last_known_enrolled_at"),
        F.dense_rank().over(w_all_by_enrolled_at_2).alias("enrolled_at_dense_rank"),
        F.first("enrolled_at").over(w_course_id_term_by_enrolled_a).alias("first_enrolled_at"),
        F.row_number().over(w_all_by_enrolled_at).alias("rn"),
        F.dense_rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_dense_rank_2"),
        F.lag("enrolled_at").over(w_all_by_enrolled_at_3).alias("prev_enrolled_at"),
        F.lag("enrolled_at").over(w_all_by_enrolled_at).alias("prev_enrolled_at_2"),
        F.lead("enrolled_at").over(w_all_by_enrolled_at_2).alias("next_enrolled_at"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at_3))) / 60, 1).alias("minutes_since_prev_enrolled_at_2"),
        F.ntile(4).over(w_all_by_enrolled_at_3).alias("enrolled_at_quartile"),
    )
)

result = (
    metrics
    .select(
        "id",
        "enrolled_at",
        "term_slug",
        "prev2_enrolled_at",
        "last_known_enrolled_at",
        "enrolled_at_dense_rank",
        "first_enrolled_at",
        "rn",
        "enrolled_at_dense_rank_2",
        "prev_enrolled_at_2",
        "next_enrolled_at",
        "minutes_since_prev_enrolled_at_2",
        "enrolled_at_quartile",
        (F.col("enrolled_at") + F.expr("INTERVAL 7 DAYS")).alias("enrolled_at_plus_7d"),
    )
    .orderBy(F.col("enrolled_at").desc_nulls_first())
)
