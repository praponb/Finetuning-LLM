from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_code_len_credits_by_c = Window.partitionBy("course_code_len", "credits").orderBy("course_code_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_credits_dept = Window.partitionBy("credits", "dept")
w_all_by_course_code_len = Window.orderBy("course_code_len", "id")
w_level = Window.partitionBy("level")
w_credits_by_credits_full = Window.partitionBy("credits").orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_course_code_len_2 = Window.orderBy(F.col("course_code_len").desc(), "id")
w_level_by_credits_ties = Window.partitionBy("level").orderBy(F.col("credits").desc())
w_dept = Window.partitionBy("dept")
w_all_by_course_code_len_ties = Window.orderBy(F.col("course_code_len").desc())

scoped_courses = (
    df_courses
    .filter((F.col("dept") == "Art") & (F.lower(F.col("course_code")).like("%a1%")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.substring(F.trim(F.col("course_code")), 1, 2).alias("course_code_prefix"),
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
    )
)

ranked = (
    scoped_courses
    .select(
        "*",
        F.first("credits", ignorenulls=True).over(w_course_code_len_credits_by_c).alias("first_known_credits"),
        F.count(F.lit(1)).over(w_credits_dept).alias("rows_in_credits_dept"),
        F.round(F.col("course_code_len") / F.nullif(F.sum("course_code_len").over(w_credits_dept), F.lit(0)), 4).alias("course_code_len_share_of_credits_dept"),
        F.dense_rank().over(w_all_by_course_code_len).alias("course_code_len_dense_rank"),
        F.round(F.avg("course_code_len").over(w_level), 2).alias("level_avg_course_code_len"),
        F.round(F.avg("credits").over(w_credits_dept), 2).alias("credits_dept_avg_credits"),
        F.last("credits").over(w_credits_by_credits_full).alias("last_credits"),
        (F.col("credits") - F.lag("credits").over(w_all_by_course_code_len_2)).alias("credits_change"),
        F.max("credits").over(w_credits_dept).alias("credits_dept_max_credits"),
        F.dense_rank().over(w_level_by_credits_ties).alias("credits_dense_rank"),
        F.round(F.col("credits") - F.avg("credits").over(w_dept), 2).alias("credits_vs_dept_avg"),
        F.round(F.cume_dist().over(w_all_by_course_code_len_ties), 3).alias("course_code_len_cume_dist"),
        F.sum("course_code_len").over(w_credits_dept).alias("credits_dept_sum_course_code_len"),
        F.lead("course_code_len", 1, 0).over(w_all_by_course_code_len).alias("next_course_code_len"),
    )
)

result = (
    ranked
    .filter(F.col("credits_dept_sum_course_code_len") > 30)
    .select(
        "id",
        "course_code_prefix",
        "first_known_credits",
        "course_code_len_share_of_credits_dept",
        "level_avg_course_code_len",
        "credits_dept_avg_credits",
        "last_credits",
        "credits_change",
        "credits_dept_max_credits",
        "credits_dense_rank",
        "course_code_len_cume_dist",
        "credits_dept_sum_course_code_len",
    )
    .orderBy(F.col("credits_dept_avg_credits").desc())
)
