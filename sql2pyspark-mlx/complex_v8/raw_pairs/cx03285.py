from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_course_id_term = Window.partitionBy("course_id", "term")
w_term_enrolled_at_dayofweek = Window.partitionBy("term", "enrolled_at_dayofweek")
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_enrolled_at_to_date_by_score = Window.partitionBy(F.to_date("enrolled_at")).orderBy(F.col("score_distance").desc(), "id").rowsBetween(-2, -1)

recent = (
    df_enrollments
    .filter((F.col("term") == "2024T1") & (F.col("grade_status") == "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 38.5).alias("score_distance"),
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
    )
)

joined_course = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", "c.course_code")
)

with_windows = (
    joined_course
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "enrolled_at_dayofweek",
        "course_code",
        (F.col("enrolled_at_dayofweek") - F.lag("enrolled_at_dayofweek").over(w_grade_status_by_enrolled_at)).alias("enrolled_at_dayofweek_change"),
        F.lag("enrolled_at", 3).over(w_all_by_enrolled_at).alias("prev3_enrolled_at"),
        F.count(F.lit(1)).over(w_course_id_term).alias("rows_in_course_id_term"),
        F.size(F.collect_set("grade_status").over(w_term_enrolled_at_dayofweek)).alias("distinct_grade_status_in_term_enrolled_at_dayofw"),
        F.round(F.col("enrolled_at_dayofweek") - F.avg("enrolled_at_dayofweek").over(w_term_enrolled_at_dayofweek), 2).alias("enrolled_at_dayofweek_vs_term_enrolled_at_dayofw"),
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_course_id_term), F.lit(0)), 4).alias("score_distance_share_of_course_id_term"),
        F.count(F.lit(1)).over(w_term_enrolled_at_dayofweek).alias("rows_in_term_enrolled_at_dayofweek"),
        F.sum("enrolled_at_dayofweek").over(w_term_enrolled_at_dayofweek).alias("term_enrolled_at_dayofweek_sum_enrolled_at_dayof"),
        F.round(F.avg("score_distance").over(w_grade_status_by_enrolled_at_2), 2).alias("score_distance_avg_2"),
        F.round(F.avg("enrolled_at_dayofweek").over(w_enrolled_at_to_date_by_score), 2).alias("enrolled_at_dayofweek_avg_2_prior"),
    )
)

derived = (
    with_windows
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
    )
)

result = (
    derived
    .filter(F.col("enrolled_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "grade_status",
        "score_distance",
        "enrolled_at_dayofweek",
        "course_code",
        "enrolled_at_dayofweek_change",
        "rows_in_course_id_term",
        "distinct_grade_status_in_term_enrolled_at_dayofw",
        "enrolled_at_dayofweek_vs_term_enrolled_at_dayofw",
        "score_distance_share_of_course_id_term",
        "rows_in_term_enrolled_at_dayofweek",
        "term_enrolled_at_dayofweek_sum_enrolled_at_dayof",
        "grade_status_slug",
        "grade_status_or_unknown",
        "term_prefix",
    )
    .orderBy(
        F.col("rows_in_course_id_term").desc(),
        F.col("enrolled_at_dayofweek").desc(),
    )
)
