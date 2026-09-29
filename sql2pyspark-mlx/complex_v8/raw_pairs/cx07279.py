from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_enrolled_at_year = Window.partitionBy("student_id", "enrolled_at_year").orderBy(F.col("term_or_unknown_len").desc())
w_student_id_enrolled_at_year_2 = Window.partitionBy("student_id", "enrolled_at_year").orderBy(F.col("term_or_unknown_len").desc(), "id")

base_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

enriched = (
    base_enrollments
    .select(
        "*",
        F.round(F.col("enrolled_at_year") / F.nullif(F.col("score"), F.lit(0)), 3).alias("enrolled_at_year_per_score"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

tagged = (
    enriched
    .select(
        "*",
        F.length(F.trim(F.col("term_or_unknown"))).alias("term_or_unknown_len"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.dense_rank().over(w_student_id_enrolled_at_year).alias("term_or_unknown_len_dense_rank"),
        F.round(F.percent_rank().over(w_student_id_enrolled_at_year_2), 3).alias("term_or_unknown_len_percent_rank"),
    )
)

rollup_base = (
    sequenced
    .groupBy("term_or_unknown_len_dense_rank")
    .agg(
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.round(F.sum("enrolled_at_year") / F.nullif(F.sum("term_or_unknown_len_dense_rank"), F.lit(0)), 3).alias("enrolled_at_year_to_term_or_unknown_len_dense_ra"),
        F.max("enrolled_at_year_per_score").alias("max_enrolled_at_year_per_score"),
        F.round(F.sum("term_or_unknown_len") / F.nullif(F.sum("term_or_unknown_len_dense_rank"), F.lit(0)), 3).alias("term_or_unknown_len_to_term_or_unknown_len_dense"),
        F.sum(F.when(F.col("grade_status").isNotNull(), F.col("term_or_unknown_len_percent_rank")).otherwise(0)).alias("term_or_unknown_len_percent_rank_grade_status_is"),
        F.sum(F.when(F.col("grade_status") == "INCOMPLETE", F.col("term_or_unknown_len_percent_rank"))).alias("term_or_unknown_len_percent_rank_grade_status_in"),
    )
)

result = (
    rollup_base
    .select(
        "term_or_unknown_len_dense_rank",
        "max_enrolled_at_year_per_score",
        "term_or_unknown_len_to_term_or_unknown_len_dense",
        "enrolled_at_year_to_term_or_unknown_len_dense_ra",
        "term_or_unknown_len_percent_rank_grade_status_is",
    )
    .unpivot(
        ids=["term_or_unknown_len_dense_rank"],
        values=["max_enrolled_at_year_per_score", "term_or_unknown_len_to_term_or_unknown_len_dense", "enrolled_at_year_to_term_or_unknown_len_dense_ra", "term_or_unknown_len_percent_rank_grade_status_is"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
