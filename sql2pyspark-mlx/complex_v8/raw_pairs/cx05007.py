from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dept_len_min_dept_len_by_min = Window.partitionBy("dept_len", "min_dept_len").orderBy(F.col("min_dept_len").desc(), "dept_len")
w_dept_len_min_dept_len = Window.partitionBy("dept_len", "min_dept_len")

filtered = (
    df_courses
    .filter((F.col("dept") != "Bio") & (F.col("level").isin("intro", "advanced")))
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        F.length(F.trim(F.col("dept"))).alias("dept_len"),
        F.lower(F.regexp_replace(F.trim(F.col("course_code")), " ", "_")).alias("course_code_slug"),
    )
)

totals = (
    filtered
    .groupBy("dept_len")
    .agg(
        F.round(F.avg("credits"), 2).alias("avg_credits"),
        F.array_sort(F.collect_set("course_code")).alias("course_code_list"),
        F.sum(F.when(F.col("course_code").isNotNull(), F.col("credits"))).alias("credits_course_code_is_not_null"),
        F.array_sort(F.collect_set("level")).alias("level_list"),
        F.min("dept_len").alias("min_dept_len"),
        F.sum("credits").alias("sum_credits_check"),
    )
    .filter(F.col("sum_credits_check") > 2)
    .drop("sum_credits_check")
)

sequenced = (
    totals
    .select(
        "*",
        F.row_number().over(w_dept_len_min_dept_len_by_min).alias("seq_no"),
        F.round(F.col("avg_credits") / F.nullif(F.sum("avg_credits").over(w_dept_len_min_dept_len), F.lit(0)), 4).alias("avg_credits_share_of_dept_len_min_dept_len"),
        F.round(F.avg("min_dept_len").over(w_dept_len_min_dept_len), 2).alias("dept_len_min_dept_len_avg_min_dept_len"),
        F.sum("min_dept_len").over(w_dept_len_min_dept_len).alias("dept_len_min_dept_len_sum_min_dept_len"),
    )
)

derived = (
    sequenced
    .filter(F.col("min_dept_len") >= 3)
    .select(
        "dept_len",
        "avg_credits",
        "course_code_list",
        "credits_course_code_is_not_null",
        "min_dept_len",
        "avg_credits_share_of_dept_len_min_dept_len",
        "dept_len_min_dept_len_sum_min_dept_len",
        (F.col("seq_no") - F.col("min_dept_len")).alias("seq_no_minus_min_dept_len"),
        F.round(F.col("dept_len") / F.nullif(F.col("min_dept_len"), F.lit(0)), 3).alias("dept_len_per_min_dept_len"),
    )
)

filtered_2 = (
    derived
    .crossJoin(derived.agg(F.min("seq_no_minus_min_dept_len").alias("overall_min_seq_no_minus_min_dept_len")))
    .filter(F.col("seq_no_minus_min_dept_len") > F.col("overall_min_seq_no_minus_min_dept_len"))
    .drop("overall_min_seq_no_minus_min_dept_len")
)

without_matches = (
    filtered_2
    .crossJoin(filtered_2.agg(F.max("avg_credits_share_of_dept_len_min_dept_len").alias("overall_max_avg_credits_share_of_dept_len_min_de")))
    .filter(F.col("avg_credits_share_of_dept_len_min_dept_len") < F.col("overall_max_avg_credits_share_of_dept_len_min_de"))
    .drop("overall_max_avg_credits_share_of_dept_len_min_de")
)

result = (
    without_matches
    .select(
        "dept_len",
        "course_code_list",
        "credits_course_code_is_not_null",
        "min_dept_len",
        "dept_len_min_dept_len_sum_min_dept_len",
        "dept_len_per_min_dept_len",
        F.least(F.col("min_dept_len"), F.col("avg_credits")).alias("least_min_dept_len_avg_credits"),
    )
    .orderBy(F.col("credits_course_code_is_not_null").desc())
)
