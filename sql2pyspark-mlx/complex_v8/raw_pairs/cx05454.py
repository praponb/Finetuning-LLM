from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term")
w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_grade_status_enrolled_at_mon = Window.partitionBy("grade_status", F.month("enrolled_at")).orderBy(F.col("enrolled_at").desc(), "id")
w_student_id_term = Window.partitionBy("student_id", "term")
w_all_by_enrolled_at_moving_6 = Window.orderBy("enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_grade_status_enrolled_at_mon_2 = Window.partitionBy("grade_status", F.month("enrolled_at")).orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_student_id_enrolled_at_quart = Window.partitionBy("student_id", "enrolled_at_quarter").orderBy(F.col("enrolled_at_quarter").desc())
w_enrolled_at_month = Window.partitionBy(F.month("enrolled_at"))
w_all_by_enrolled_at_quarter_m = Window.orderBy(F.col("enrolled_at_quarter_max_7").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_enrolled_at_quarter_m_2 = Window.orderBy(F.col("enrolled_at_quarter_max_7").asc_nulls_last(), "id")

base = (
    df_enrollments
    .filter((F.col("score") > 16.3) & (F.col("enrolled_at") >= F.lit("2025-01-31 15:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

sequenced = (
    base
    .select(
        "*",
        F.min("enrolled_at_quarter").over(w_term).alias("term_min_enrolled_at_quarter"),
        F.ntile(3).over(w_term_by_enrolled_at).alias("enrolled_at_tercile"),
        F.lead("enrolled_at", 3).over(w_all_by_enrolled_at).alias("next3_enrolled_at"),
        F.ntile(10).over(w_grade_status_enrolled_at_mon).alias("enrolled_at_decile"),
        F.round(F.col("enrolled_at_quarter") / F.nullif(F.sum("enrolled_at_quarter").over(w_student_id_term), F.lit(0)), 4).alias("enrolled_at_quarter_share_of_student_id_term"),
        F.max("enrolled_at_quarter").over(w_all_by_enrolled_at_moving_6).alias("enrolled_at_quarter_max_7"),
        F.sum("enrolled_at_quarter").over(w_grade_status_enrolled_at_mon_2).alias("enrolled_at_quarter_sum_3"),
        F.dense_rank().over(w_student_id_enrolled_at_quart).alias("enrolled_at_quarter_dense_rank"),
        F.lag("enrolled_at_quarter").over(w_term_by_enrolled_at).alias("prev_enrolled_at_quarter"),
        F.sum("enrolled_at_quarter").over(w_enrolled_at_month).alias("enrolled_at_month_sum_enrolled_at_quarter"),
        F.lead("enrolled_at_quarter", 2).over(w_grade_status_enrolled_at_mon).alias("next2_enrolled_at_quarter"),
    )
)

top_per_group = sequenced.filter(F.col("enrolled_at_quarter_dense_rank") <= 3)

with_trends = (
    top_per_group
    .select(
        "*",
        F.round(F.avg("enrolled_at_quarter").over(w_all_by_enrolled_at_quarter_m), 2).alias("enrolled_at_quarter_avg_4"),
        F.row_number().over(w_all_by_enrolled_at_quarter_m_2).alias("enrolled_at_quarter_max_7_seq"),
        (F.col("next2_enrolled_at_quarter") - F.lag("next2_enrolled_at_quarter").over(w_all_by_enrolled_at_quarter_m_2)).alias("next2_enrolled_at_quarter_change"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "enrolled_at_quarter",
        "term_min_enrolled_at_quarter",
        "next3_enrolled_at",
        "enrolled_at_decile",
        "enrolled_at_quarter_share_of_student_id_term",
        "enrolled_at_quarter_max_7",
        "enrolled_at_quarter_sum_3",
        "enrolled_at_quarter_dense_rank",
        "prev_enrolled_at_quarter",
        "enrolled_at_month_sum_enrolled_at_quarter",
        "next2_enrolled_at_quarter",
        "enrolled_at_quarter_avg_4",
        F.date_format(F.col("next3_enrolled_at"), "MMM yyyy").alias("next3_enrolled_at_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("next3_enrolled_at")).alias("days_since_next3_enrolled_at"),
    )
    .orderBy("enrolled_at_quarter_sum_3")
)
