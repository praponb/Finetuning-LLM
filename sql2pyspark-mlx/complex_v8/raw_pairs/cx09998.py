from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits_dept = Window.partitionBy("credits", "dept")
w_credits_dept_by_credits = Window.partitionBy("credits", "dept").orderBy("credits", "id")
w_all_by_credits_ties = Window.orderBy("credits")
w_all_by_credits_full = Window.orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_credits_level = Window.partitionBy("credits", "level")
w_credits_by_credits_ties = Window.partitionBy("credits").orderBy(F.col("credits").asc_nulls_last())

filtered = df_courses.filter((F.col("credits") > 2) & (F.col("dept") != "Bio"))

sequenced = (
    filtered
    .select(
        "*",
        F.count(F.lit(1)).over(w_credits_dept).alias("rows_in_credits_dept"),
        F.row_number().over(w_credits_dept_by_credits).alias("seq_no"),
        F.dense_rank().over(w_credits_dept_by_credits).alias("credits_dense_rank"),
        F.rank().over(w_all_by_credits_ties).alias("credits_rank"),
        F.nth_value("credits", 2).over(w_all_by_credits_full).alias("second_credits"),
        F.round(F.avg("credits").over(w_credits_level), 2).alias("credits_level_avg_credits"),
        F.round(F.percent_rank().over(w_all_by_credits_ties), 3).alias("credits_percent_rank"),
        F.ntile(4).over(w_credits_dept_by_credits).alias("credits_quartile"),
        F.size(F.collect_set("dept").over(w_credits_level)).alias("distinct_dept_in_credits_level"),
        F.rank().over(w_credits_by_credits_ties).alias("credits_rank_2"),
    )
)

result = (
    sequenced
    .filter(F.col("credits").between(2, 4))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "credits_dense_rank",
        "credits_rank",
        "second_credits",
        "credits_level_avg_credits",
        "credits_quartile",
        "distinct_dept_in_credits_level",
        F.concat_ws("-", F.col("course_code"), F.col("level")).alias("course_code_level_label"),
    )
    .orderBy(F.col("credits_level_avg_credits").desc())
)
