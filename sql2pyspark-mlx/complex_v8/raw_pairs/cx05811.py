from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits_level_or_unknown = Window.partitionBy("credits", "level_or_unknown")
w_dept_level = Window.partitionBy("dept", "level")
w_credits_by_credits_moving_3 = Window.partitionBy("credits").orderBy("credits", "id").rowsBetween(-3, Window.currentRow)
w_dept_by_credits = Window.partitionBy("dept").orderBy(F.col("credits").asc_nulls_last(), "id")
w_credits_by_credits = Window.partitionBy("credits").orderBy(F.col("credits").desc(), "id")
w_credits_by_credits_ties = Window.partitionBy("credits").orderBy("credits")
w_level_or_unknown_by_credits = Window.partitionBy("level_or_unknown").orderBy(F.col("credits").asc_nulls_last(), "id")
w_credits_by_credits_2 = Window.partitionBy("credits").orderBy("credits", "id")
w_dept_by_credits_ties = Window.partitionBy("dept").orderBy(F.col("credits").asc_nulls_last())
w_all_by_credits_rank_2_ties = Window.orderBy("credits_rank_2")
w_distinct_level_in_credits_le = Window.partitionBy("distinct_level_in_credits_level_or_unknown")
w_level_or_unknown_by_distinct = Window.partitionBy("level_or_unknown").orderBy("distinct_dept_in_credits_level_or_unknown", "id")
w_level_or_unknown_by_distinct_2 = Window.partitionBy("level_or_unknown").orderBy("distinct_dept_in_credits_level_or_unknown", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_or_unknown_by_distinct_3 = Window.partitionBy("level_or_unknown").orderBy("distinct_dept_in_credits_level_or_unknown")

filtered = (
    df_courses
    .filter(F.col("dept").isin("Bio", "Math", "Art"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
        F.initcap(F.trim(F.col("course_code"))).alias("course_code_clean"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.size(F.collect_set("dept").over(w_credits_level_or_unknown)).alias("distinct_dept_in_credits_level_or_unknown"),
        F.round(F.col("credits") - F.avg("credits").over(w_dept_level), 2).alias("credits_vs_dept_level_avg"),
        F.round(F.avg("credits").over(w_credits_by_credits_moving_3), 2).alias("credits_avg_4"),
        F.ntile(5).over(w_dept_by_credits).alias("credits_quintile"),
        F.row_number().over(w_credits_by_credits).alias("rn"),
        F.count(F.lit(1)).over(w_credits_level_or_unknown).alias("rows_in_credits_level_or_unknown"),
        F.rank().over(w_credits_by_credits_ties).alias("credits_rank"),
        F.lag("credits", 2).over(w_level_or_unknown_by_credits).alias("prev2_credits"),
        F.round(F.avg("credits").over(w_credits_level_or_unknown), 2).alias("credits_level_or_unknown_avg_credits"),
        (F.col("credits") - F.lag("credits").over(w_credits_by_credits_2)).alias("credits_change"),
        F.rank().over(w_dept_by_credits_ties).alias("credits_rank_2"),
        F.max("credits").over(w_dept_level).alias("dept_level_max_credits"),
        F.size(F.collect_set("level").over(w_credits_level_or_unknown)).alias("distinct_level_in_credits_level_or_unknown"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w_all_by_credits_rank_2_ties).alias("credits_rank_2_dense_rank"),
        F.min("credits_change").over(w_distinct_level_in_credits_le).alias("distinct_level_in_credits_level_or_unknown_min_c"),
        F.round(F.percent_rank().over(w_level_or_unknown_by_distinct), 3).alias("distinct_dept_in_credits_level_or_unknown_percen"),
        F.sum("credits_avg_4").over(w_level_or_unknown_by_distinct_2).alias("running_credits_avg_4"),
        F.round(F.percent_rank().over(w_level_or_unknown_by_distinct_3), 3).alias("distinct_dept_in_credits_level_or_unknown_percen_2"),
        F.size(F.collect_set("dept").over(w_distinct_level_in_credits_le)).alias("distinct_dept_in_distinct_level_in_credits_level"),
        F.sum("credits_change").over(w_distinct_level_in_credits_le).alias("distinct_level_in_credits_level_or_unknown_sum_c"),
        F.round(F.avg("credits").over(w_distinct_level_in_credits_le), 2).alias("distinct_level_in_credits_level_or_unknown_avg_c"),
    )
)

totals = (
    metrics_2
    .filter(F.col("distinct_dept_in_distinct_level_in_credits_level") >= 5)
    .groupBy(
        "credits_rank_2_dense_rank",
        "distinct_dept_in_credits_level_or_unknown",
        "credits_rank_2",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("dept")).alias("dept_list"),
        F.countDistinct("dept").alias("distinct_depts"),
        F.round(F.sum("prev2_credits") / F.nullif(F.sum("credits_change"), F.lit(0)), 3).alias("prev2_credits_to_credits_change_ratio"),
        F.countDistinct("level_or_unknown").alias("distinct_level_or_unknowns"),
    )
)

result = (
    totals
    .filter(F.col("prev2_credits_to_credits_change_ratio") > 3.0)
    .select(
        "credits_rank_2_dense_rank",
        "distinct_dept_in_credits_level_or_unknown",
        "credits_rank_2",
        "cnt",
        "dept_list",
        "distinct_depts",
        "prev2_credits_to_credits_change_ratio",
        "distinct_level_or_unknowns",
    )
    .orderBy(F.col("credits_rank_2_dense_rank").desc(), F.col("cnt").desc())
)
