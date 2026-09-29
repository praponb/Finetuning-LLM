from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level")
w_grp = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("dept_id").orderBy(F.col("running_max_salary").desc(), "id")
w_hist = Window.partitionBy("dept_id").orderBy("running_min_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("dept_id").orderBy(F.col("running_sum_level_count_salary").desc(), "id")
w7 = Window.partitionBy("running_min_level_count_salary_tier").orderBy(F.col("running_sum_level_count_salary_per_engineering").desc())

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-09-06").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "is_remote",
        "level",
        F.substring("full_name", 1, 4).alias("full_name_prefix"),
    )
)

with_windows = (
    filtered
    .filter(F.lower(F.trim("full_name")) == "jack taylor")
    .select(
        "id",
        "dept_id",
        "salary",
        "is_remote",
        F.min("salary").over(w_ordered).alias("running_min_salary"),
        F.avg("salary").over(w_ordered).alias("running_avg_salary"),
        F.max("salary").over(w_ordered).alias("running_max_salary"),
        F.count("salary").over(w2).alias("level_count_salary"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "dept_id",
        "salary",
        "is_remote",
        "running_min_salary",
        "running_avg_salary",
        "running_max_salary",
        "level_count_salary",
        F.avg("running_min_salary").over(w_grp).alias("running_min_salary_avg_2"),
        F.ntile(10).over(w4).alias("running_max_salary_bucket"),
    )
)

with_windows_employees = (
    enriched
    .select(
        "id",
        "dept_id",
        "is_remote",
        F.avg("level_count_salary").over(w_hist).alias("running_avg_level_count_salary"),
        F.min("level_count_salary").over(w_hist).alias("running_min_level_count_salary"),
        F.sum("level_count_salary").over(w_hist).alias("running_sum_level_count_salary"),
    )
)

ranked = (
    with_windows_employees
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    ranked
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.budget"), F.col("b.dept_name"), F.col("b.location"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("running_min_level_count_salary") >= 16.8, "LARGE").otherwise("SMALL").alias("running_min_level_count_salary_tier"),
        F.when(F.col("budget") >= 3791000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.round(F.sum("running_sum_level_count_salary") / F.nullif(F.count(F.when(F.col("dept_name") == "Engineering", 1)), F.lit(0)), 2).alias("running_sum_level_count_salary_per_engineering"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.percentile_approx("running_min_level_count_salary", [0.25, 0.5, 0.75]).alias("running_min_level_count_salary_quartiles"),
    )
    .withColumn("running_sum_level_count_salary_per_engineering_dense_rank", F.dense_rank().over(w7))
)
