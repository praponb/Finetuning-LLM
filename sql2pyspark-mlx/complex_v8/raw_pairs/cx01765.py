from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_score_running = Window.partitionBy("course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id = Window.partitionBy("course_id")
w_term_by_enrolled_at_ties = Window.partitionBy("term").orderBy(F.col("enrolled_at").desc())
w_course_id_enrolled_at_month = Window.partitionBy("course_id", F.date_trunc("month", "enrolled_at")).orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_year_term_by_sco = Window.partitionBy(F.year("enrolled_at"), "term").orderBy("score", "id")
w_enrolled_at_year = Window.partitionBy(F.year("enrolled_at"))
w_course_id_enrolled_at_month_2 = Window.partitionBy("course_id", F.date_trunc("month", "enrolled_at")).orderBy(F.col("score").desc())

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") != "WITHDRAWN")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

ranked = (
    cleaned_enrollments
    .select(
        "*",
        F.last("score", ignorenulls=True).over(w_course_id_by_score_running).alias("last_known_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.rank().over(w_term_by_enrolled_at_ties).alias("enrolled_at_rank"),
        F.last("enrolled_at", ignorenulls=True).over(w_course_id_enrolled_at_month).alias("last_known_enrolled_at"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
        F.lag("enrolled_at", 2).over(w_enrolled_at_year_term_by_sco).alias("prev2_enrolled_at"),
        F.max("score").over(w_enrolled_at_year).alias("enrolled_at_year_max_score"),
        F.rank().over(w_course_id_enrolled_at_month_2).alias("score_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_year), F.lit(0)), 4).alias("score_share_of_enrolled_at_year"),
        F.count(F.lit(1)).over(w_enrolled_at_year).alias("rows_in_enrolled_at_year"),
    )
)

scored = (
    ranked
    .filter(F.col("term").isin("2024T2", "2025T1"))
    .select(
        "*",
        (F.col("score_vs_course_id_avg") - F.col("last_known_score")).alias("score_vs_course_id_avg_minus_last_known_score"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label_2"),
    )
)

result = (
    scored
    .filter(F.col("rows_in_enrolled_at_year") <= 12)
    .select(
        "id",
        "course_id",
        "score",
        "last_known_score",
        "score_share_of_course_id",
        "enrolled_at_rank",
        "last_known_enrolled_at",
        "prev2_enrolled_at",
        "enrolled_at_year_max_score",
        "score_rank",
        "score_share_of_enrolled_at_year",
        "rows_in_enrolled_at_year",
        "score_vs_course_id_avg_minus_last_known_score",
        "enrolled_at_label",
        "enrolled_at_label_2",
        F.lower(F.regexp_replace(F.trim(F.col("enrolled_at_label_2")), " ", "_")).alias("enrolled_at_label_2_slug"),
    )
    .orderBy(F.col("enrolled_at_label_2").desc(), F.col("id").asc_nulls_last())
    .limit(10)
)
