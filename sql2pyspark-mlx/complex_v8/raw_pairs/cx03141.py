from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_plus_1d_month = Window.partitionBy(F.month("enrolled_at_plus_1d"))
w_student_id_course_id_by_enro = Window.partitionBy("student_id", "course_id").orderBy("enrolled_at", "id")
w_enrolled_at_plus_1d_year = Window.partitionBy(F.year("enrolled_at_plus_1d"))
w_student_id_course_id_by_enro_2 = Window.partitionBy("student_id", "course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_by_enrolled_at_plu = Window.partitionBy("course_id").orderBy("enrolled_at_plus_1d")
w_enrolled_at_plus_1d_year_cou = Window.partitionBy(F.year("enrolled_at_plus_1d"), "course_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_enrolled_at_plus_1d_year_cou_2 = Window.partitionBy(F.year("enrolled_at_plus_1d"), "course_id").orderBy(F.col("enrolled_at").desc(), "id")
w_term = Window.partitionBy("term")
w_enrolled_at_plus_1d_rank_ter = Window.partitionBy("enrolled_at_plus_1d_rank", "term").orderBy("enrolled_at_percent_rank", "id")
w_enrolled_at_plus_1d_rank_ter_2 = Window.partitionBy("enrolled_at_plus_1d_rank", "term").orderBy("enrolled_at_percent_rank", "id").rowsBetween(-6, Window.currentRow)
w_course_id_by_score_sum_7 = Window.partitionBy("course_id").orderBy(F.col("score_sum_7").desc_nulls_first(), "id")

cleaned = (
    df_enrollments
    .filter((F.col("course_id") >= 4) & (F.col("enrolled_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 1 DAYS")).alias("enrolled_at_plus_1d"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.count(F.lit(1)).over(w_enrolled_at_plus_1d_month).alias("rows_in_enrolled_at_plus_1d_month"),
        F.round(F.percent_rank().over(w_student_id_course_id_by_enro), 3).alias("enrolled_at_percent_rank"),
        F.sum("score").over(w_enrolled_at_plus_1d_year).alias("enrolled_at_plus_1d_year_sum_score"),
        F.lag("score").over(w_student_id_course_id_by_enro).alias("prev_score"),
        F.sum("score").over(w_student_id_course_id_by_enro_2).alias("running_score"),
        F.rank().over(w_course_id_by_enrolled_at_plu).alias("enrolled_at_plus_1d_rank"),
        F.sum("score").over(w_enrolled_at_plus_1d_year_cou).alias("score_sum_7"),
        F.dense_rank().over(w_enrolled_at_plus_1d_year_cou_2).alias("enrolled_at_dense_rank"),
    )
)

first_rows = with_trends.filter(F.col("enrolled_at_dense_rank") <= 2)

ranked = (
    first_rows
    .select(
        "*",
        F.round(F.col("running_score") / F.nullif(F.sum("running_score").over(w_term), F.lit(0)), 4).alias("running_score_share_of_term"),
        (F.col("running_score") - F.lag("running_score").over(w_enrolled_at_plus_1d_rank_ter)).alias("running_score_change"),
        F.ntile(4).over(w_enrolled_at_plus_1d_rank_ter).alias("enrolled_at_percent_rank_quartile"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.round(F.avg("enrolled_at_plus_1d_rank").over(w_enrolled_at_plus_1d_rank_ter_2), 2).alias("enrolled_at_plus_1d_rank_avg_7"),
        F.row_number().over(w_course_id_by_score_sum_7).alias("score_sum_7_seq"),
        F.max("enrolled_at_plus_1d_rank").over(w_term).alias("term_max_enrolled_at_plus_1d_rank"),
    )
)

totals = (
    ranked
    .filter(F.col("enrolled_at_dense_rank") >= 3)
    .groupBy(
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "enrolled_at_percent_rank_quartile",
        "student_id",
    )
    .agg(
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.sum(F.when(F.col("term") == "2024T2", F.col("enrolled_at_percent_rank_quartile"))).alias("enrolled_at_percent_rank_quartile_term_2024t2"),
        F.round(F.stddev("enrolled_at_percent_rank_quartile"), 2).alias("stddev_enrolled_at_percent_rank_quartile"),
        F.avg("enrolled_at_plus_1d_year_sum_score").alias("avg_enrolled_at_plus_1d_year_sum_score_check"),
    )
    .filter(F.col("avg_enrolled_at_plus_1d_year_sum_score_check") > 350.0)
    .drop("avg_enrolled_at_plus_1d_year_sum_score_check")
)

result = (
    totals
    .select(
        "enrolled_at_month",
        "enrolled_at_percent_rank_quartile",
        "student_id",
        "term_list",
        "stddev_enrolled_at_percent_rank_quartile",
        F.date_trunc("quarter", F.col("enrolled_at_month")).alias("enrolled_at_month_quarter"),
        (F.col("enrolled_at_month") + F.expr("INTERVAL 1 DAYS")).alias("enrolled_at_month_plus_1d"),
    )
    .orderBy(
        "enrolled_at_month_quarter",
        F.col("student_id").desc(),
        "enrolled_at_month",
        "enrolled_at_percent_rank_quartile",
    )
    .limit(5)
)
