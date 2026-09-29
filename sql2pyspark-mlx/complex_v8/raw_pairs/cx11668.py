from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits_dept_len_by_credits = Window.partitionBy("credits", "dept_len").orderBy(F.col("credits").desc(), "id")
w_dept_credits_filled = Window.partitionBy("dept", "credits_filled")
w_credits_dept_len_by_credits_2 = Window.partitionBy("credits", "dept_len").orderBy(F.col("credits").desc(), "id").rowsBetween(-3, -1)
w_credits_dept_len_by_credits_3 = Window.partitionBy("credits", "dept_len").orderBy(F.col("credits").desc())
w_dept_credits_by_credits_fill = Window.partitionBy("dept", "credits").orderBy("credits_filled")
w_dept_credits_by_credits_fill_2 = Window.partitionBy("dept", "credits").orderBy("credits_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dept_credits_by_credits_fill_3 = Window.partitionBy("dept", "credits").orderBy("credits_filled", "id")
w_credits_filled_dept_len_by_d = Window.partitionBy("credits_filled", "dept_len").orderBy("dept_len", "id")
w_dept_credits_by_credits_fill_4 = Window.partitionBy("dept", "credits").orderBy("credits_filled", "id").rowsBetween(-6, Window.currentRow)
w_credits_filled_credits_fille = Window.partitionBy("credits_filled", "credits_filled_rank").orderBy("min_credits_filled_so_far", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_credits_sum_7 = Window.partitionBy("credits_sum_7")
w_all_by_credits_sum_7_prior_1 = Window.orderBy("credits_sum_7", "id").rowsBetween(-1, -1)

cleaned_courses = (
    df_courses
    .filter(F.col("credits") < 3)
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        F.length(F.trim(F.col("dept"))).alias("dept_len"),
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
    )
)

sequenced = (
    cleaned_courses
    .select(
        "*",
        F.lag("credits_filled", 2).over(w_credits_dept_len_by_credits).alias("prev2_credits_filled"),
        F.max("dept_len").over(w_credits_dept_len_by_credits_2).alias("dept_len_max_3_prior"),
        F.round(F.cume_dist().over(w_credits_dept_len_by_credits_3), 3).alias("credits_cume_dist"),
        F.round(F.col("dept_len") / F.nullif(F.sum("dept_len").over(w_dept_credits_filled), F.lit(0)), 4).alias("dept_len_share_of_dept_credits_filled"),
        F.round(F.cume_dist().over(w_dept_credits_by_credits_fill), 3).alias("credits_filled_cume_dist"),
        F.min("credits_filled").over(w_dept_credits_by_credits_fill_2).alias("min_credits_filled_so_far"),
        F.round(F.percent_rank().over(w_dept_credits_by_credits_fill_3), 3).alias("credits_filled_percent_rank"),
        F.round(F.cume_dist().over(w_credits_filled_dept_len_by_d), 3).alias("dept_len_cume_dist"),
        F.rank().over(w_dept_credits_by_credits_fill).alias("credits_filled_rank"),
        F.sum("credits").over(w_dept_credits_by_credits_fill_4).alias("credits_sum_7"),
    )
)

ranked = (
    sequenced
    .select(
        "*",
        F.nth_value("credits_sum_7", 2).over(w_credits_filled_credits_fille).alias("second_credits_sum_7"),
        F.round(F.col("dept_len_cume_dist") - F.avg("dept_len_cume_dist").over(w_credits_sum_7), 2).alias("dept_len_cume_dist_vs_credits_sum_7_avg"),
        F.sum("prev2_credits_filled").over(w_all_by_credits_sum_7_prior_1).alias("prev2_credits_filled_sum_1_prior"),
        F.sum("dept_len_max_3_prior").over(w_credits_sum_7).alias("credits_sum_7_sum_dept_len_max_3_prior"),
    )
)

result = (
    ranked
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "credits_filled",
        "prev2_credits_filled",
        "dept_len_max_3_prior",
        "credits_cume_dist",
        "dept_len_share_of_dept_credits_filled",
        "min_credits_filled_so_far",
        "credits_filled_percent_rank",
        "second_credits_sum_7",
        "prev2_credits_filled_sum_1_prior",
        "credits_sum_7_sum_dept_len_max_3_prior",
        F.least(F.col("dept_len_cume_dist"), F.col("credits_sum_7")).alias("least_dept_len_cume_dist_credits_sum_7"),
    )
)
