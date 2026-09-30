from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_len_by_term_len = Window.partitionBy("term_len").orderBy(F.col("term_len").desc(), "id")
w_term = Window.partitionBy("term")
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")

valid = (
    df_enrollments
    .filter((F.col("student_id") <= 3) & (F.col("course_id") <= 5))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 71.0, "large").when(F.col("score") >= 32.7, "medium").otherwise("small").alias("score_tier"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

labeled = (
    valid
    .filter(F.col("term_len").between(6, 11))
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.length(F.trim(F.col("score_tier"))).alias("score_tier_len"),
    )
)

windowed = (
    labeled
    .select(
        "*",
        F.lead("score_tier_len").over(w_term_len_by_term_len).alias("next_score_tier_len"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.round(F.col("score_tier_len") - F.avg("score_tier_len").over(w_term), 2).alias("score_tier_len_vs_term_avg"),
        F.first("score_tier_len", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("first_known_score_tier_len"),
        F.lag("enrolled_at", 3).over(w_all_by_enrolled_at).alias("prev3_enrolled_at"),
    )
)

scored = (
    windowed
    .filter(F.col("term_len") < 9)
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "score_tier",
        "term_len",
        "grade_status_slug",
        "score_tier_len",
        "rows_in_term",
        "score_tier_len_vs_term_avg",
        "first_known_score_tier_len",
        "prev3_enrolled_at",
        F.round(F.col("rows_in_term") / F.nullif(F.col("score_tier_len"), F.lit(0)), 3).alias("rows_in_term_per_score_tier_len"),
        F.coalesce(F.col("next_score_tier_len"), F.lit(0)).alias("next_score_tier_len_filled"),
        F.coalesce(F.col("score_tier_len"), F.lit(0)).alias("score_tier_len_filled"),
    )
)

joined = (
    scored.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_slug",
        "s.score_tier_len",
        "s.rows_in_term",
        "s.score_tier_len_vs_term_avg",
        "s.prev3_enrolled_at",
        "s.rows_in_term_per_score_tier_len",
        "s.next_score_tier_len_filled",
        "s.score_tier_len_filled",
        "c.course_code",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

result = (
    joined
    .select(
        "id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "grade_status_slug",
        "score_tier_len",
        "rows_in_term",
        "score_tier_len_vs_term_avg",
        "prev3_enrolled_at",
        "rows_in_term_per_score_tier_len",
        "course_code",
        "course_dept",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev3_enrolled_at")).alias("days_since_prev3_enrolled_at"),
        F.round(F.col("score_tier_len") / F.nullif(F.col("score_tier_len_filled"), F.lit(0)), 3).alias("score_tier_len_per_score_tier_len_filled"),
    )
    .orderBy("score_tier_len_vs_term_avg")
)
