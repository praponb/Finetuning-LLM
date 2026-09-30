from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_term_enrolled_at_year = Window.partitionBy("term", F.year("enrolled_at"))
w_term_course_id_by_enrolled_a = Window.partitionBy("term", "course_id").orderBy(F.col("enrolled_at").desc(), "id")
w_term_course_id_by_enrolled_a_2 = Window.partitionBy("term", "course_id").orderBy(F.col("enrolled_at").desc())
w_course_id = Window.partitionBy("course_id")
w_student_id_by_enrolled_at_mo = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(-3, Window.currentRow)
w_term_course_id_by_enrolled_a_3 = Window.partitionBy("term", "course_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "term",
        "score",
        "enrolled_at",
        "term_ref",
        (F.col("score") - F.lag("score").over(w_student_id_by_enrolled_at)).alias("score_change"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term_enrolled_at_year), F.lit(0)), 4).alias("score_share_of_term_enrolled_at_year"),
        F.lead("score", 3).over(w_term_course_id_by_enrolled_a).alias("next3_score"),
        F.round(F.cume_dist().over(w_term_course_id_by_enrolled_a_2), 3).alias("enrolled_at_cume_dist"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.round(F.avg("score").over(w_student_id_by_enrolled_at_mo), 2).alias("score_avg_4"),
        F.max("score").over(w_term_enrolled_at_year).alias("term_enrolled_at_year_max_score"),
        F.sum("score").over(w_term_course_id_by_enrolled_a_3).alias("running_score"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        (F.col("enrolled_at_cume_dist") + F.col("score_share_of_course_id")).alias("enrolled_at_cume_dist_plus_score_share_of_course"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

result = (
    scored
    .select(
        "id",
        "term",
        "score",
        "enrolled_at",
        "term_ref",
        "score_change",
        "score_share_of_term_enrolled_at_year",
        "score_avg_4",
        "term_enrolled_at_year_max_score",
        "running_score",
        "enrolled_at_cume_dist_plus_score_share_of_course",
        "term_or_unknown",
    )
    .orderBy(F.col("enrolled_at").desc())
)
