from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_credits = Window.orderBy(F.col("credits").desc(), "id")
w_dept_level_by_credits_runnin = Window.partitionBy("dept", "level").orderBy(F.col("credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_credits_moving_1 = Window.orderBy(F.col("credits").desc(), "id").rowsBetween(-1, Window.currentRow)
w_level_by_credits_ties = Window.partitionBy("level").orderBy(F.col("credits").desc_nulls_first())
w_dept = Window.partitionBy("dept")
w_level_by_credits_prior_6 = Window.partitionBy("level").orderBy(F.col("credits").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_all_by_credits_moving_2 = Window.orderBy(F.col("credits").desc(), "id").rowsBetween(-2, Window.currentRow)
w_credits_max_6_prior = Window.partitionBy("credits_max_6_prior")
w_credits_max_6_prior_by_credi = Window.partitionBy("credits_max_6_prior").orderBy("credits_avg_3", "id")
w_credits_max_6_prior_by_credi_2 = Window.partitionBy("credits_max_6_prior").orderBy("credits_avg_3", "id").rowsBetween(-3, Window.currentRow)
w_credits_max_6_prior_by_credi_3 = Window.partitionBy("credits_max_6_prior").orderBy(F.col("credits_avg_3").asc_nulls_last(), "id")

filtered = (
    df_courses
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.concat_ws("-", F.col("dept"), F.col("level")).alias("dept_level_label"),
        F.concat_ws("|", F.col("course_code"), F.col("dept")).alias("course_code_dept_label"),
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.rank().over(w_all_by_credits).alias("credits_rank"),
        F.first("credits", ignorenulls=True).over(w_dept_level_by_credits_runnin).alias("first_known_credits"),
        F.round(F.avg("credits").over(w_all_by_credits_moving_1), 2).alias("credits_avg_2"),
        F.round(F.percent_rank().over(w_level_by_credits_ties), 3).alias("credits_percent_rank"),
        F.size(F.collect_set("level").over(w_dept)).alias("distinct_level_in_dept"),
        F.max("credits").over(w_dept).alias("dept_max_credits"),
        F.max("credits").over(w_level_by_credits_prior_6).alias("credits_max_6_prior"),
        F.round(F.avg("credits").over(w_all_by_credits_moving_2), 2).alias("credits_avg_3"),
        F.count(F.lit(1)).over(w_dept).alias("rows_in_dept"),
        F.round(F.col("credits") - F.avg("credits").over(w_dept), 2).alias("credits_vs_dept_avg"),
        F.round(F.cume_dist().over(w_all_by_credits), 3).alias("credits_cume_dist"),
        F.ntile(10).over(w_all_by_credits).alias("credits_decile"),
    )
)

scored = (
    sequenced
    .select(
        "id",
        "credits",
        "level",
        "credits_avg_2",
        "distinct_level_in_dept",
        "credits_max_6_prior",
        "credits_avg_3",
        "credits_vs_dept_avg",
        F.concat(F.col("course_code_dept_label"), F.lit("#"), F.col("first_known_credits").cast("string")).alias("course_code_dept_label_ref"),
    )
)

sequenced_2 = (
    scored
    .select(
        "*",
        F.round(F.col("distinct_level_in_dept") - F.avg("distinct_level_in_dept").over(w_credits_max_6_prior), 2).alias("distinct_level_in_dept_vs_credits_max_6_prior_av"),
        F.max("credits_vs_dept_avg").over(w_credits_max_6_prior).alias("credits_max_6_prior_max_credits_vs_dept_avg"),
        (F.col("credits_vs_dept_avg") - F.lag("credits_vs_dept_avg").over(w_credits_max_6_prior_by_credi)).alias("credits_vs_dept_avg_change"),
        F.ntile(10).over(w_all_by_credits).alias("credits_decile"),
        F.round(F.avg("credits_vs_dept_avg").over(w_credits_max_6_prior_by_credi_2), 2).alias("credits_vs_dept_avg_avg_4"),
        F.row_number().over(w_credits_max_6_prior_by_credi_3).alias("seq_no"),
        F.ntile(10).over(w_credits_max_6_prior_by_credi_3).alias("credits_avg_3_decile"),
    )
)

result = (
    sequenced_2
    .filter(F.col("credits") <= 3)
    .select(
        "id",
        "credits",
        "level",
        "distinct_level_in_dept",
        "credits_max_6_prior",
        "credits_avg_3",
        "credits_vs_dept_avg",
        "course_code_dept_label_ref",
        "credits_max_6_prior_max_credits_vs_dept_avg",
        "credits_vs_dept_avg_change",
        "credits_decile",
        "credits_vs_dept_avg_avg_4",
        "seq_no",
        "credits_avg_3_decile",
        F.substring(F.trim(F.col("level")), 1, 3).alias("level_prefix"),
    )
    .orderBy("credits_vs_dept_avg_avg_4")
)
