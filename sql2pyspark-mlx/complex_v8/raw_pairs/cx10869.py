from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_major_score_tier_by = Window.partitionBy("student_major", "score_tier").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_student_is_internationa = Window.partitionBy("term", "student_is_international").orderBy(F.col("enrolled_at").desc())
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at")
w_student_major_score_tier_by_2 = Window.partitionBy("student_major", "score_tier").orderBy(F.col("enrolled_at").desc(), "id")
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_enrolled_at_dense_rank_enrol = Window.partitionBy("enrolled_at_dense_rank", "enrolled_at_decile")
w_term_score_tier_by_enrolled = Window.partitionBy("term", "score_tier").orderBy(F.col("enrolled_at_decile").desc(), "id")
w_next2_enrolled_at_year_by_ne = Window.partitionBy(F.year("next2_enrolled_at")).orderBy("next3_enrolled_at", "id")

scoped_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) | (F.to_date(F.col("enrolled_at")) < F.lit("2025-02-06").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 69.6, "A").when(F.col("score") >= 13.9, "B").otherwise("C").alias("score_tier"),
    )
)

with_parent = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
    )
)

sequenced = (
    with_parent
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "score_tier",
        "student_is_international",
        "student_major",
        F.last("enrolled_at").over(w_student_major_score_tier_by).alias("last_enrolled_at"),
        F.round(F.cume_dist().over(w_term_student_is_internationa), 3).alias("enrolled_at_cume_dist"),
        F.lead("enrolled_at").over(w_all_by_enrolled_at).alias("next_enrolled_at"),
        F.nth_value("enrolled_at", 2).over(w_all_by_enrolled_at_full).alias("second_enrolled_at"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.ntile(10).over(w_all_by_enrolled_at).alias("enrolled_at_decile"),
        F.dense_rank().over(w_grade_status_by_enrolled_at).alias("enrolled_at_dense_rank"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_major_score_tier_by_2))) / 60, 1).alias("minutes_since_prev_enrolled_at_2"),
        F.lead("enrolled_at", 2).over(w_all_by_enrolled_at).alias("next2_enrolled_at"),
        F.lead("enrolled_at", 3).over(w_grade_status_by_enrolled_at_2).alias("next3_enrolled_at"),
    )
)

labeled = (
    sequenced
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("next3_enrolled_at")).alias("days_since_next3_enrolled_at"),
    )
)

sequenced_2 = (
    labeled
    .select(
        "*",
        F.count(F.lit(1)).over(w_enrolled_at_dense_rank_enrol).alias("rows_in_enrolled_at_dense_rank_enrolled_at_decil"),
        F.round(F.col("days_since_next3_enrolled_at") / F.nullif(F.sum("days_since_next3_enrolled_at").over(w_enrolled_at_dense_rank_enrol), F.lit(0)), 4).alias("days_since_next3_enrolled_at_share_of_enrolled_a"),
        F.lag("second_enrolled_at").over(w_term_score_tier_by_enrolled).alias("prev_second_enrolled_at"),
        F.rank().over(w_next2_enrolled_at_year_by_ne).alias("next3_enrolled_at_rank"),
    )
)

result = (
    sequenced_2
    .filter(~F.col("student_major").isin("CS", "Biz"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "score_tier",
        "student_is_international",
        "student_major",
        "next_enrolled_at",
        "second_enrolled_at",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_dense_rank",
        "minutes_since_prev_enrolled_at_2",
        "next2_enrolled_at",
        "next3_enrolled_at",
        "days_since_next3_enrolled_at",
        "rows_in_enrolled_at_dense_rank_enrolled_at_decil",
        "prev_second_enrolled_at",
        F.substring(F.trim(F.col("score_tier")), 1, 3).alias("score_tier_prefix"),
    )
    .orderBy(
        "days_since_next3_enrolled_at",
        F.col("rows_in_enrolled_at_dense_rank_enrolled_at_decil").asc(),
    )
)
