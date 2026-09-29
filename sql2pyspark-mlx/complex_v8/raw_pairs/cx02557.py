from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_grade_status_by_e = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term = Window.partitionBy("term")
w_dedup = Window.partitionBy("student_id").orderBy("last_known_days_since_enrolled_at", "id")
w_n_rows = Window.partitionBy("n_rows")
w_term_by_total_last_known_day = Window.partitionBy("term").orderBy(F.col("total_last_known_days_since_enrolled_at").desc(), "term", "grade_status", "enrolled_at_day")
w_term_by_n_rows_running = Window.partitionBy("term").orderBy(F.col("n_rows").desc(), "term", "grade_status", "enrolled_at_day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_total_last_known_day_2 = Window.partitionBy("term").orderBy(F.col("total_last_known_days_since_enrolled_at").desc(), "term", "grade_status", "enrolled_at_day").rowsBetween(-1, Window.currentRow)
w_all_by_last_known_n_rows_tie = Window.orderBy(F.col("last_known_n_rows").desc_nulls_first())
w_all_by_last_known_n_rows = Window.orderBy(F.col("last_known_n_rows").desc_nulls_first(), "term", "grade_status", "enrolled_at_day")
w_all_by_last_known_n_rows_pri = Window.orderBy(F.col("last_known_n_rows").desc_nulls_first(), "term", "grade_status", "enrolled_at_day").rowsBetween(-1, -1)
w_last_known_n_rows_term = Window.partitionBy("last_known_n_rows", "term")

valid = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

windowed = (
    valid
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "term_or_unknown",
        F.last("days_since_enrolled_at", ignorenulls=True).over(w_student_id_grade_status_by_e).alias("last_known_days_since_enrolled_at"),
        F.size(F.collect_set("course_id").over(w_term)).alias("distinct_course_id_in_term"),
        F.size(F.collect_set("grade_status").over(w_term)).alias("distinct_grade_status_in_term"),
    )
)

unique_rows = (
    windowed
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

student_id_stats = (
    unique_rows
    .groupBy("student_id")
    .agg(F.max("last_known_days_since_enrolled_at").alias("max_last_known_days_since_enrolled_at"))
)

above_average = (
    unique_rows.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.last_known_days_since_enrolled_at") == F.col("g.max_last_known_days_since_enrolled_at"))
    .select(
        "s.*",
        "g.max_last_known_days_since_enrolled_at",
        F.round(F.col("s.last_known_days_since_enrolled_at") - F.col("g.max_last_known_days_since_enrolled_at"), 2).alias("last_known_days_since_enrolled_at_minus_max"),
    )
)

summary = (
    above_average
    .filter(F.col("max_last_known_days_since_enrolled_at") <= 385.0)
    .groupBy("term", "grade_status", F.to_date("enrolled_at").alias("enrolled_at_day"))
    .agg(
        F.sum("last_known_days_since_enrolled_at").alias("total_last_known_days_since_enrolled_at"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("last_known_days_since_enrolled_at_minus_max") / F.nullif(F.sum("max_last_known_days_since_enrolled_at"), F.lit(0)), 3).alias("last_known_days_since_enrolled_at_minus_max_to_m"),
    )
)

ranked = (
    summary
    .select(
        "*",
        F.count(F.lit(1)).over(w_n_rows).alias("rows_in_n_rows"),
        F.lead("n_rows").over(w_term_by_total_last_known_day).alias("next_n_rows"),
        F.last("n_rows", ignorenulls=True).over(w_term_by_n_rows_running).alias("last_known_n_rows"),
        F.max("n_rows").over(w_term_by_total_last_known_day_2).alias("n_rows_max_2"),
    )
)

sequenced = (
    ranked
    .select(
        "term",
        "grade_status",
        "enrolled_at_day",
        "total_last_known_days_since_enrolled_at",
        "n_rows",
        "last_known_days_since_enrolled_at_minus_max_to_m",
        "rows_in_n_rows",
        "next_n_rows",
        "last_known_n_rows",
        F.round(F.cume_dist().over(w_all_by_last_known_n_rows_tie), 3).alias("last_known_n_rows_cume_dist"),
        F.row_number().over(w_all_by_last_known_n_rows).alias("last_known_n_rows_seq"),
        F.sum("rows_in_n_rows").over(w_all_by_last_known_n_rows_pri).alias("rows_in_n_rows_sum_1_prior"),
        F.round(F.col("n_rows_max_2") - F.avg("n_rows_max_2").over(w_last_known_n_rows_term), 2).alias("n_rows_max_2_vs_last_known_n_rows_term_avg"),
    )
)

result = (
    sequenced
    .cube("grade_status", "last_known_n_rows_seq")
    .agg(
        F.sum(F.when(~F.col("grade_status").isin("PASS", "FAIL"), 1).otherwise(0)).alias("n_grade_status_not_in_pass_fai"),
        F.max("last_known_days_since_enrolled_at_minus_max_to_m").alias("max_last_known_days_since_enrolled_at_minus_max_"),
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("last_known_n_rows_seq").asc_nulls_last(),
    )
)
