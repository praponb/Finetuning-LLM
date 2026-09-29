from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_dept_len_by_credits = Window.partitionBy("level", "dept_len").orderBy("credits", "id")
w_credits_dept_len = Window.partitionBy("credits", "dept_len")

recent = (
    df_courses
    .filter(F.col("dept") == "Biz")
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.concat_ws("-", F.col("dept"), F.col("course_code")).alias("dept_course_code_label"),
        F.upper(F.trim(F.col("dept"))).alias("dept_clean"),
        F.length(F.trim(F.col("dept"))).alias("dept_len"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.round(F.percent_rank().over(w_level_dept_len_by_credits), 3).alias("credits_percent_rank"),
        F.round(F.col("dept_len") / F.nullif(F.sum("dept_len").over(w_credits_dept_len), F.lit(0)), 4).alias("dept_len_share_of_credits_dept_len"),
        F.round(F.col("dept_len") - F.avg("dept_len").over(w_credits_dept_len), 2).alias("dept_len_vs_credits_dept_len_avg"),
    )
)

result = (
    metrics
    .groupBy("credits")
    .pivot("level", ["intro", "core", "advanced"])
    .agg(F.max("credits_percent_rank"))
)
