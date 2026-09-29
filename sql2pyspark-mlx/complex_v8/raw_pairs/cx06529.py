from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at_plus_1d", "id")
w_term = Window.partitionBy("term")
w_all_by_enrolled_at_plus_1d = Window.orderBy(F.col("enrolled_at_plus_1d").asc_nulls_last(), "id")
w_course_id_term_by_enrolled_a = Window.partitionBy("course_id", "term").orderBy(F.col("enrolled_at_plus_1d").desc(), "id")
w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy(F.col("enrolled_at_plus_1d").desc())
w_grade_status_term_by_enrolle = Window.partitionBy("grade_status", "term").orderBy("enrolled_at", "id").rowsBetween(-1, -1)
w_course_id_term_by_enrolled_a_2 = Window.partitionBy("course_id", "term").orderBy(F.col("enrolled_at_plus_1d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("enrolled_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 1 DAYS")).alias("enrolled_at_plus_1d"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        (F.col("score") - F.lag("score").over(w_grade_status_by_enrolled_at)).alias("score_change"),
        F.size(F.collect_set("course_id").over(w_term)).alias("distinct_course_id_in_term"),
        F.row_number().over(w_all_by_enrolled_at_plus_1d).alias("rn"),
        F.lead("enrolled_at").over(w_course_id_term_by_enrolled_a).alias("next_enrolled_at"),
        F.round(F.cume_dist().over(w_course_id_term_by_enrolled_a), 3).alias("enrolled_at_plus_1d_cume_dist"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.dense_rank().over(w_course_id_grade_status_by_en).alias("enrolled_at_plus_1d_dense_rank"),
        F.ntile(10).over(w_all_by_enrolled_at_plus_1d).alias("enrolled_at_plus_1d_decile"),
        F.sum("score").over(w_grade_status_term_by_enrolle).alias("score_sum_1_prior"),
        F.last("enrolled_at", ignorenulls=True).over(w_course_id_term_by_enrolled_a_2).alias("last_known_enrolled_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_plus_1d",
        "score_change",
        "distinct_course_id_in_term",
        "rn",
        "next_enrolled_at",
        "enrolled_at_plus_1d_cume_dist",
        "rows_in_term",
        "score_sum_1_prior",
        "last_known_enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
    .orderBy("id")
)
