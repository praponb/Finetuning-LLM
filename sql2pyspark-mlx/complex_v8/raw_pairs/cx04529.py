from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_score_ties = Window.partitionBy("grade_status").orderBy("score")
w_term_group_term = Window.partitionBy("term_group", "term")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at_prior_3 = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_course_id_term_group = Window.partitionBy("course_id", "term_group")
w_student_id_by_enrolled_at_mo = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_term = Window.partitionBy("term")
w_student_id_by_enrolled_at_ti = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc())

prepared_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") == "INCOMPLETE")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2024T2", "2025T2"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
    )
)

sequenced = (
    prepared_enrollments
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "term_group",
        "term_clean",
        F.rank().over(w_grade_status_by_score_ties).alias("score_rank"),
        F.round(F.avg("score").over(w_term_group_term), 2).alias("term_group_term_avg_score"),
        (F.col("score") - F.lag("score").over(w_all_by_enrolled_at)).alias("score_change"),
        F.round(F.cume_dist().over(w_student_id_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
        F.round(F.avg("score").over(w_all_by_enrolled_at_prior_3), 2).alias("score_avg_3_prior"),
        F.lead("enrolled_at").over(w_student_id_by_enrolled_at).alias("next_enrolled_at"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id_term_group), F.lit(0)), 4).alias("score_share_of_course_id_term_group"),
        F.min("score").over(w_term_group_term).alias("term_group_term_min_score"),
        F.sum("score").over(w_student_id_by_enrolled_at_mo).alias("score_sum_2"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist_2"),
        F.min("score").over(w_term).alias("term_min_score"),
        F.round(F.col("score") - F.avg("score").over(w_term_group_term), 2).alias("score_vs_term_group_term_avg"),
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.rank().over(w_student_id_by_enrolled_at_ti).alias("enrolled_at_rank_2"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "term_group",
        "term_clean",
        "term_group_term_avg_score",
        "score_change",
        "enrolled_at_cume_dist",
        "enrolled_at_cume_dist_2",
        "term_min_score",
        "score_vs_term_group_term_avg",
        "rows_in_term",
        "enrolled_at_rank_2",
        F.coalesce(F.col("rows_in_term"), F.lit(0)).alias("rows_in_term_filled"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
    .orderBy(F.col("days_since_enrolled_at").asc_nulls_last(), "id")
    .limit(10)
)
