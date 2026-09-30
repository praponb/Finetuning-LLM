from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_row_count_grade_status = Window.partitionBy("row_count", "grade_status")
w_distinct_ids = Window.partitionBy("distinct_ids")
w_all_by_n_term_is_not_null = Window.orderBy("n_term_is_not_null", "grade_status", "enrolled_at_month", "enrolled_at_quarter_month")
w_all_by_n_term_is_not_null_fu = Window.orderBy(F.col("n_term_is_not_null").asc_nulls_last(), "grade_status", "enrolled_at_month", "enrolled_at_quarter_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_enrollments = (
    df_enrollments
    .filter(F.col("grade_status").isin("PASS", "WITHDRAWN", "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

grouped = (
    prepared_enrollments
    .groupBy(
        "grade_status",
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        F.date_trunc("month", "enrolled_at_quarter").alias("enrolled_at_quarter_month"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(100 * F.count(F.when(F.col("grade_status") == "WITHDRAWN", 1)) / F.count("*"), 1).alias("pct_grade_status_withdrawn"),
        F.sum(F.when(F.col("term").isNotNull(), 1).otherwise(0)).alias("n_term_is_not_null"),
    )
)

with_windows = (
    grouped
    .select(
        "*",
        F.round(F.avg("pct_grade_status_withdrawn").over(w_row_count_grade_status), 2).alias("row_count_grade_status_avg_pct_grade_status_with"),
        F.round(F.avg("distinct_ids").over(w_row_count_grade_status), 2).alias("row_count_grade_status_avg_distinct_ids"),
        F.count(F.lit(1)).over(w_row_count_grade_status).alias("rows_in_row_count_grade_status"),
        F.round(F.col("n_term_is_not_null") / F.nullif(F.sum("n_term_is_not_null").over(w_row_count_grade_status), F.lit(0)), 4).alias("n_term_is_not_null_share_of_row_count_grade_stat"),
        F.max("distinct_ids").over(w_row_count_grade_status).alias("row_count_grade_status_max_distinct_ids"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.round(F.col("distinct_ids") - F.avg("distinct_ids").over(w_distinct_ids), 2).alias("distinct_ids_vs_distinct_ids_avg"),
        F.ntile(4).over(w_all_by_n_term_is_not_null).alias("n_term_is_not_null_quartile"),
        F.first("enrolled_at_quarter_month").over(w_all_by_n_term_is_not_null_fu).alias("first_enrolled_at_quarter_month"),
    )
)

result = (
    with_windows_2
    .select(
        "grade_status",
        "enrolled_at_month",
        F.col("n_term_is_not_null_share_of_row_count_grade_stat").alias("m_n_term_is_not_null_share_of_row_count_grade_st"),
        F.col("row_count_grade_status_avg_pct_grade_status_with").alias("row_count_grade_status_avg_pct_grade_status_with_2"),
        F.col("row_count_grade_status_avg_distinct_ids").alias("m_row_count_grade_status_avg_distinct_ids"),
    )
    .unpivot(
        ids=["grade_status", "enrolled_at_month"],
        values=["m_n_term_is_not_null_share_of_row_count_grade_st", "row_count_grade_status_avg_pct_grade_status_with_2", "m_row_count_grade_status_avg_distinct_ids"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("grade_status", "enrolled_at_month", "field")
)
