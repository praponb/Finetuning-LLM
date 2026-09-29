from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_credits_running = Window.orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_credits_dept_by_credits_movi = Window.partitionBy("credits", "dept").orderBy("credits", "id").rowsBetween(-6, Window.currentRow)
w_all_by_credits_ties = Window.orderBy(F.col("credits").desc())
w_credits = Window.partitionBy("credits")
w_all_by_credits = Window.orderBy("credits", "id")
w_all_by_credits_2 = Window.orderBy(F.col("credits").desc(), "id")
w_all_by_credits_ties_2 = Window.orderBy("credits")
w_credits_rank = Window.partitionBy("credits_rank")
w_distinct_level_in_credits_cr = Window.partitionBy("distinct_level_in_credits", "credits_max_7").orderBy(F.col("seq_no").asc_nulls_last())
w_distinct_level_in_credits_by = Window.partitionBy("distinct_level_in_credits").orderBy(F.col("credits_max_7").desc_nulls_first(), "id").rowsBetween(-3, -1)

base_courses = df_courses

ranked = (
    base_courses
    .select(
        "id",
        "credits",
        F.first("credits", ignorenulls=True).over(w_all_by_credits_running).alias("first_known_credits"),
        F.max("credits").over(w_credits_dept_by_credits_movi).alias("credits_max_7"),
        F.rank().over(w_all_by_credits_ties).alias("credits_rank"),
        F.round(F.col("credits") - F.avg("credits").over(w_credits), 2).alias("credits_vs_credits_avg"),
        F.ntile(10).over(w_all_by_credits).alias("credits_decile"),
        F.row_number().over(w_all_by_credits_2).alias("seq_no"),
        F.min("credits").over(w_credits).alias("credits_min_credits"),
        F.size(F.collect_set("level").over(w_credits)).alias("distinct_level_in_credits"),
        F.round(F.cume_dist().over(w_all_by_credits_ties_2), 3).alias("credits_cume_dist"),
        F.count(F.lit(1)).over(w_credits).alias("rows_in_credits"),
        F.sum("credits").over(w_credits).alias("credits_sum_credits"),
        F.size(F.collect_set("dept").over(w_credits)).alias("distinct_dept_in_credits"),
    )
)

first_rows = ranked.filter(F.col("seq_no") == 1)

with_trends = (
    first_rows
    .select(
        "id",
        "credits",
        "first_known_credits",
        "credits_max_7",
        "credits_vs_credits_avg",
        "credits_decile",
        "seq_no",
        "credits_min_credits",
        "distinct_level_in_credits",
        "rows_in_credits",
        "credits_sum_credits",
        F.count(F.lit(1)).over(w_credits_rank).alias("rows_in_credits_rank"),
        F.round(F.col("credits_vs_credits_avg") / F.nullif(F.sum("credits_vs_credits_avg").over(w_credits_rank), F.lit(0)), 4).alias("credits_vs_credits_avg_share_of_credits_rank"),
        F.round(F.col("credits_max_7") / F.nullif(F.sum("credits_max_7").over(w_credits_rank), F.lit(0)), 4).alias("credits_max_7_share_of_credits_rank"),
        F.round(F.percent_rank().over(w_distinct_level_in_credits_cr), 3).alias("seq_no_percent_rank"),
        F.round(F.avg("credits_sum_credits").over(w_distinct_level_in_credits_by), 2).alias("credits_sum_credits_avg_3_prior"),
        F.round(F.col("seq_no") / F.nullif(F.sum("seq_no").over(w_credits_rank), F.lit(0)), 4).alias("seq_no_share_of_credits_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "credits",
        "first_known_credits",
        "credits_max_7",
        "credits_vs_credits_avg",
        "credits_decile",
        "seq_no",
        "credits_min_credits",
        "rows_in_credits",
        "credits_sum_credits",
        "rows_in_credits_rank",
        "credits_vs_credits_avg_share_of_credits_rank",
        "seq_no_percent_rank",
        "seq_no_share_of_credits_rank",
        F.when(F.col("credits_max_7") >= 7, "A").when(F.col("credits_max_7") >= 5, "B").otherwise("C").alias("credits_max_7_tier"),
    )
    .orderBy("first_known_credits", F.col("id").desc_nulls_first())
)
