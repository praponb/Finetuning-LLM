from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_enrolled_at_fu = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_enrolled_at_ti = Window.partitionBy("student_id").orderBy("enrolled_at")
w_student_id = Window.partitionBy("student_id")
w_student_id_by_enrolled_at_ru = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_score_ties = Window.partitionBy("student_id").orderBy(F.col("score").desc_nulls_first())
w_course_id = Window.partitionBy("course_id")
w_student_id_by_score = Window.partitionBy("student_id").orderBy(F.col("score").desc_nulls_first(), "id")
w_dedup = Window.partitionBy("term", "course_id").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

recent = (
    df_enrollments
    .filter(F.to_date(F.col("enrolled_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.nth_value("days_since_enrolled_at", 2).over(w_student_id_by_enrolled_at_fu).alias("second_days_since_enrolled_at"),
        F.rank().over(w_student_id_by_enrolled_at_ti).alias("enrolled_at_rank"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.last("enrolled_at", ignorenulls=True).over(w_student_id_by_enrolled_at_ru).alias("last_known_enrolled_at"),
    )
)

sequenced = (
    metrics
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "enrolled_at_rank",
        "rows_in_student_id",
        F.rank().over(w_student_id_by_score_ties).alias("score_rank"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.round(F.cume_dist().over(w_student_id_by_score), 3).alias("score_cume_dist"),
        F.row_number().over(w_student_id_by_score).alias("score_seq"),
        F.round(F.col("days_since_enrolled_at") / F.nullif(F.sum("days_since_enrolled_at").over(w_course_id), F.lit(0)), 4).alias("days_since_enrolled_at_share_of_course_id"),
    )
)

unique_rows = (
    sequenced
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .cube("enrolled_at_rank", "term")
    .agg(F.count("*").alias("n_rows"))
)
