from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_grade_status_term_by_enrolle = Window.partitionBy("grade_status", "term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_term_by_enrolle_2 = Window.partitionBy("grade_status", "term").orderBy("enrolled_at")
w_grade_status_term_by_enrolle_3 = Window.partitionBy("grade_status", "term").orderBy("enrolled_at", "id")
w_enrolled_at_to_date_term_by = Window.partitionBy(F.to_date("enrolled_at"), "term").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_enrolled_at_to_date_term_by_2 = Window.partitionBy(F.to_date("enrolled_at"), "term").orderBy(F.col("enrolled_at").desc())
w_term_group_grade_status_by_e = Window.partitionBy("term_group", "grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2025T1", "2024T1"), "group_a").when(F.col("term") == "2024T2", "group_b").otherwise("other").alias("term_group"),
    )
)

metrics = (
    prepared_enrollments
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank"),
        F.first("enrolled_at").over(w_grade_status_term_by_enrolle).alias("first_enrolled_at"),
        F.round(F.cume_dist().over(w_grade_status_term_by_enrolle_2), 3).alias("enrolled_at_cume_dist"),
        F.lead("enrolled_at", 2).over(w_grade_status_term_by_enrolle_3).alias("next2_enrolled_at"),
        F.ntile(5).over(w_enrolled_at_to_date_term_by).alias("enrolled_at_quintile"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_grade_status_term_by_enrolle_3))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.lag("enrolled_at").over(w_all_by_enrolled_at).alias("prev_enrolled_at"),
        F.row_number().over(w_enrolled_at_to_date_term_by).alias("rn"),
        F.ntile(10).over(w_all_by_enrolled_at).alias("enrolled_at_decile"),
        F.rank().over(w_enrolled_at_to_date_term_by_2).alias("enrolled_at_rank_2"),
        F.last("enrolled_at", ignorenulls=True).over(w_term_group_grade_status_by_e).alias("last_known_enrolled_at"),
        F.lag("enrolled_at").over(w_enrolled_at_to_date_term_by).alias("prev_enrolled_at_2"),
        F.ntile(4).over(w_all_by_enrolled_at).alias("enrolled_at_quartile"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist_2"),
    )
)

first_rows = (
    metrics
    .filter((F.col("enrolled_at_rank") <= 3) & (F.col("enrolled_at_percent_rank") < 0.8))
    .drop("enrolled_at_rank")
)

result = (
    first_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "term_group",
        "first_enrolled_at",
        "enrolled_at_cume_dist",
        "enrolled_at_quintile",
        "prev_enrolled_at",
        "rn",
        "enrolled_at_decile",
        "enrolled_at_rank_2",
        "last_known_enrolled_at",
        "prev_enrolled_at_2",
        "enrolled_at_quartile",
        "enrolled_at_cume_dist_2",
    )
    .orderBy(F.col("id").desc(), F.col("course_id").asc())
)
