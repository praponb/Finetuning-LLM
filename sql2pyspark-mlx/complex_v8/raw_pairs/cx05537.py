from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month_course_id = Window.partitionBy(F.month("enrolled_at"), "course_id").orderBy(F.col("last_enrollment_enrolled_at").asc_nulls_last(), "id")
w_enrolled_at_month_course_id_2 = Window.partitionBy(F.month("enrolled_at"), "course_id").orderBy(F.col("last_enrollment_enrolled_at").asc_nulls_last())
w_enrolled_at_month_course_id_3 = Window.partitionBy(F.month("enrolled_at"), "course_id").orderBy(F.col("last_enrollment_enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_score = Window.partitionBy("term").orderBy("score", "id")
w_last_enrollment_enrolled_at = Window.partitionBy("last_enrollment_enrolled_at_quintile", F.to_date("last_enrollment_enrolled_at"))

base = (
    df_enrollments
    .filter((F.col("course_id") >= 9) & (F.col("term") == "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        (F.col("enrolled_at") + F.expr("INTERVAL 1 DAYS")).alias("enrolled_at_plus_1d"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("enrollment_term_count"),
    )
)

profiled = (
    base.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

windowed = (
    profiled
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_date",
        "enrolled_at_plus_1d",
        "last_enrollment_enrolled_at",
        "enrollment_term_count",
        F.ntile(5).over(w_enrolled_at_month_course_id).alias("last_enrollment_enrolled_at_quintile"),
        F.round(F.cume_dist().over(w_enrolled_at_month_course_id_2), 3).alias("last_enrollment_enrolled_at_cume_dist"),
        F.last("enrolled_at_date", ignorenulls=True).over(w_enrolled_at_month_course_id_3).alias("last_known_enrolled_at_date"),
        F.first("enrolled_at_plus_1d", ignorenulls=True).over(w_enrolled_at_month_course_id_3).alias("first_known_enrolled_at_plus_1d"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at_date",
        "enrolled_at_plus_1d",
        "enrollment_term_count",
        "last_enrollment_enrolled_at_quintile",
        "last_known_enrolled_at_date",
        "first_known_enrolled_at_plus_1d",
        F.datediff(F.col("enrolled_at_date"), F.lag("enrolled_at_date").over(w_term_by_score)).alias("days_since_prev_enrolled_at_date"),
        F.lag("enrolled_at_plus_1d", 2).over(w_term_by_score).alias("prev2_enrolled_at_plus_1d"),
        F.round(F.col("enrollment_term_count") / F.nullif(F.sum("enrollment_term_count").over(w_last_enrollment_enrolled_at), F.lit(0)), 4).alias("enrollment_term_count_share_of_last_enrollment_e"),
        F.count(F.lit(1)).over(w_last_enrollment_enrolled_at).alias("rows_in_last_enrollment_enrolled_at_quintile_las"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_last_enrollment_enrolled_at), F.lit(0)), 4).alias("score_share_of_last_enrollment_enrolled_at_quint"),
        F.lag("enrolled_at").over(w_term_by_score).alias("prev_enrolled_at"),
    )
)

line_level = (
    with_trends.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at_date",
        "s.enrolled_at_plus_1d",
        "s.last_enrollment_enrolled_at_quintile",
        "s.last_known_enrolled_at_date",
        "s.enrollment_term_count_share_of_last_enrollment_e",
        "s.rows_in_last_enrollment_enrolled_at_quintile_las",
        "s.prev_enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

result = (
    line_level
    .select(
        "id",
        "enrollment_id",
        "enrollment_score",
        "enrollment_term_count_share_of_last_enrollment_e",
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["enrollment_score", "enrollment_term_count_share_of_last_enrollment_e"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
