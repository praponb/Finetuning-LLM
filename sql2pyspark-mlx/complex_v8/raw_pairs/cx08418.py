from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dept_len_by_credits = Window.partitionBy("dept_len").orderBy(F.col("credits").asc_nulls_last(), "id")
w_dept_len_dept_group = Window.partitionBy("dept_len", "dept_group")

base = (
    df_courses
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.when(F.col("dept").isin("Biz", "CS"), "group_a").when(F.col("dept") == "Math", "group_b").otherwise("other").alias("dept_group"),
        F.length(F.trim(F.col("dept"))).alias("dept_len"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "course_code",
        "dept",
        "dept_group",
        "dept_len",
        F.lag("dept_len", 1, 0).over(w_dept_len_by_credits).alias("prev_dept_len"),
        F.min("credits").over(w_dept_len_dept_group).alias("dept_len_dept_group_min_credits"),
    )
)

enriched = (
    with_windows
    .withColumn("dept_group_or_unknown", F.coalesce(F.col("dept_group"), F.lit("UNKNOWN")))
)

result = (
    enriched
    .groupBy("dept_len", "dept_group_or_unknown")
    .pivot("dept", ["Art", "CS"])
    .agg(F.min("prev_dept_len"))
    .orderBy("dept_len", "dept_group_or_unknown")
)
