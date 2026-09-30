from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrolled_at_hou = Window.partitionBy("course_id").orderBy(F.col("enrolled_at_hour").asc_nulls_last(), "id")
w_all_by_score_running = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term = Window.partitionBy("term")

recent_enrollments = (
    df_enrollments
    .filter(F.col("course_id") >= 2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.when(F.col("score") >= 57.6, "A").when(F.col("score") >= 26.2, "B").otherwise("C").alias("score_tier"),
        F.date_trunc("hour", F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

with_trends = (
    recent_enrollments
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "score_tier",
        "enrolled_at_hour",
        F.round((F.unix_timestamp("enrolled_at_hour") - F.unix_timestamp(F.lag("enrolled_at_hour").over(w_course_id_by_enrolled_at_hou))) / 60, 1).alias("minutes_since_prev_enrolled_at_hour"),
        F.first("enrolled_at_hour", ignorenulls=True).over(w_all_by_score_running).alias("first_known_enrolled_at_hour"),
        F.sum("score").over(w_term).alias("term_sum_score"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "minutes_since_prev_enrolled_at_hour",
        "score",
        "term_sum_score",
    )
    .unpivot(
        ids=["id"],
        values=["minutes_since_prev_enrolled_at_hour", "score", "term_sum_score"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
