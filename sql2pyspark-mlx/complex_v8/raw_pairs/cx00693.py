from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_n_term_2025t2_by_max_sco = Window.partitionBy("cnt", "n_term_2025t2").orderBy(F.col("max_score_adj_2").desc(), "enrolled_at_month", "term")
w_cnt_n_term_2025t2_by_max_sco_2 = Window.partitionBy("cnt", "n_term_2025t2").orderBy(F.col("max_score_adj_2").desc(), "enrolled_at_month", "term").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_cnt_by_min_score_adj_2 = Window.partitionBy("cnt").orderBy("min_score_adj_2", "enrolled_at_month", "term")
w_enrolled_at_month_year_term = Window.partitionBy(F.year("enrolled_at_month"), "term")

valid_enrollments = (
    df_enrollments
    .filter((F.col("score") < 80.2) | (F.col("enrolled_at") < F.lit("2025-03-02 08:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.round(F.col("score") * 1.1, 2).alias("score_adj_2"),
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
    )
)

joined = (
    valid_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.dept").isin("Biz", "CS", "Art")), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_adj",
        "s.score_adj_2",
        F.col("c.credits").alias("course_credits"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

per_group = (
    joined
    .filter(F.col("grade_status").isin("INCOMPLETE", "PASS"))
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"), "term")
    .agg(
        F.max("score_adj_2").alias("max_score_adj_2"),
        F.min("score_adj_2").alias("min_score_adj_2"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("term") == "2024T2", F.col("score_adj_2"))).alias("score_adj_2_term_2024t2"),
        F.count(F.when(F.col("term") == "2025T2", 1)).alias("n_term_2025t2"),
    )
)

without_matches = (
    per_group
    .crossJoin(per_group.agg(F.max("score_adj_2_term_2024t2").alias("overall_max_score_adj_2_term_2024t2")))
    .filter(F.col("score_adj_2_term_2024t2") < F.col("overall_max_score_adj_2_term_2024t2"))
    .drop("overall_max_score_adj_2_term_2024t2")
)

with_windows = (
    without_matches
    .select(
        "enrolled_at_month",
        "term",
        "max_score_adj_2",
        "min_score_adj_2",
        "cnt",
        "score_adj_2_term_2024t2",
        "n_term_2025t2",
        F.row_number().over(w_cnt_n_term_2025t2_by_max_sco).alias("max_score_adj_2_seq"),
        F.max("score_adj_2_term_2024t2").over(w_cnt_n_term_2025t2_by_max_sco_2).alias("max_score_adj_2_term_2024t2_so_far"),
        F.lag("enrolled_at_month").over(w_cnt_by_min_score_adj_2).alias("prev_enrolled_at_month"),
        F.count(F.lit(1)).over(w_enrolled_at_month_year_term).alias("rows_in_enrolled_at_month_year_term"),
    )
)

result = (
    with_windows
    .select(
        "enrolled_at_month",
        "term",
        "max_score_adj_2_term_2024t2_so_far",
        "score_adj_2_term_2024t2",
        "min_score_adj_2",
        "max_score_adj_2",
    )
    .unpivot(
        ids=["enrolled_at_month", "term"],
        values=["max_score_adj_2_term_2024t2_so_far", "score_adj_2_term_2024t2", "min_score_adj_2", "max_score_adj_2"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .orderBy("enrolled_at_month", "term", "metric")
)
