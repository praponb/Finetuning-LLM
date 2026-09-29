from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt = Window.partitionBy("cnt")
w_score_tier_2_term_list_item = Window.partitionBy("score_tier_2", "term_list_item").orderBy(F.col("cnt").asc_nulls_last(), "enrolled_at_month", "score_tier_2", "term_list_item_pos")

recent = (
    df_enrollments
    .filter((F.col("term").isin("2025T1", "2024T2", "2025T2")) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 79.3, "HIGH").when(F.col("score") >= 23.7, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

enriched = (
    recent
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.initcap(F.trim(F.col("score_tier"))).alias("score_tier_clean"),
        F.when(F.col("score") >= 66.0, "large").when(F.col("score") >= 23.7, "medium").otherwise("small").alias("score_tier_2"),
    )
)

per_group = (
    enriched
    .groupBy(
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "score_tier_2",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
    )
)

exploded = (
    per_group
    .select(
        "enrolled_at_month",
        "score_tier_2",
        "cnt",
        "grade_status_list",
        F.posexplode("term_list").alias("term_list_item_pos", "term_list_item"),
    )
)

windowed = (
    exploded
    .select(
        "enrolled_at_month",
        "score_tier_2",
        "cnt",
        "term_list_item_pos",
        F.sum("term_list_item_pos").over(w_cnt).alias("cnt_sum_term_list_item_pos"),
        F.max("cnt").over(w_cnt).alias("cnt_max_cnt"),
        F.round(F.col("term_list_item_pos") - F.avg("term_list_item_pos").over(w_cnt), 2).alias("term_list_item_pos_vs_cnt_avg"),
        F.row_number().over(w_score_tier_2_term_list_item).alias("cnt_seq"),
    )
)

screened = (
    windowed
    .crossJoin(windowed.agg(F.avg("cnt_sum_term_list_item_pos").alias("overall_avg_cnt_sum_term_list_item_pos")))
    .filter(F.col("cnt_sum_term_list_item_pos") > F.col("overall_avg_cnt_sum_term_list_item_pos"))
    .drop("overall_avg_cnt_sum_term_list_item_pos")
)

result = (
    screened
    .rollup("cnt_seq", "cnt", "term_list_item_pos")
    .agg(
        F.grouping("cnt_seq").alias("is_total_cnt_seq"),
        F.max("enrolled_at_month").alias("last_enrolled_at_month"),
    )
    .orderBy(
        F.col("cnt_seq").asc_nulls_last(),
        F.col("cnt").asc_nulls_last(),
        F.col("term_list_item_pos").asc_nulls_last(),
    )
)
