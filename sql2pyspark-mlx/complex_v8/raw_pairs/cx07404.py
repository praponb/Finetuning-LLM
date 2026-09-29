from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dept_level = Window.partitionBy("dept", "level")
w_level_dept_by_credits_ties = Window.partitionBy("level", "dept").orderBy(F.col("credits").desc())
w_all_by_credits_full = Window.orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_credits_dept_by_credits_ties = Window.partitionBy("credits", "dept").orderBy(F.col("credits").desc())
w_credits_dept_by_credits_prio = Window.partitionBy("credits", "dept").orderBy(F.col("credits").desc(), "id").rowsBetween(-2, -1)
w_credits_dept_by_credits_runn = Window.partitionBy("credits", "dept").orderBy(F.col("credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_credits = Window.orderBy("credits", "id")
w_credits_dept_by_credits = Window.partitionBy("credits", "dept").orderBy(F.col("credits").desc(), "id")

cleaned = (
    df_courses
    .filter(F.lower(F.col("course_code")).like("%cs%"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.round(F.col("credits") / F.nullif(F.sum("credits").over(w_dept_level), F.lit(0)), 4).alias("credits_share_of_dept_level"),
        F.rank().over(w_level_dept_by_credits_ties).alias("credits_rank"),
        F.nth_value("credits", 2).over(w_all_by_credits_full).alias("second_credits"),
        F.round(F.percent_rank().over(w_credits_dept_by_credits_ties), 3).alias("credits_percent_rank"),
        F.sum("credits").over(w_credits_dept_by_credits_prio).alias("credits_sum_2_prior"),
        F.sum("credits").over(w_credits_dept_by_credits_runn).alias("running_credits"),
        F.count(F.lit(1)).over(w_dept_level).alias("rows_in_dept_level"),
        F.round(F.percent_rank().over(w_all_by_credits), 3).alias("credits_percent_rank_2"),
        F.round(F.col("credits") - F.avg("credits").over(w_dept_level), 2).alias("credits_vs_dept_level_avg"),
        F.lead("credits", 3).over(w_credits_dept_by_credits).alias("next3_credits"),
        F.rank().over(w_all_by_credits).alias("credits_rank_2"),
    )
)

derived = (
    metrics
    .filter(F.col("credits") <= 4)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("course_code")), " ", "_")).alias("course_code_slug"),
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
        F.lower(F.regexp_replace(F.trim(F.col("course_code")), " ", "_")).alias("course_code_slug_2"),
    )
)

result = (
    derived
    .select(
        "id",
        "credits",
        "level",
        "credits_share_of_dept_level",
        "credits_rank",
        "second_credits",
        "running_credits",
        "rows_in_dept_level",
        "credits_vs_dept_level_avg",
        "next3_credits",
        "credits_rank_2",
        "course_code_slug",
        "course_code_len",
    )
    .orderBy(F.col("credits").desc_nulls_first(), "id")
    .limit(5)
)
