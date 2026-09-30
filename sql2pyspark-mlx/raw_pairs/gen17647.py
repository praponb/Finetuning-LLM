from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "dept_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w_grp = Window.partitionBy("manager_id")
w3 = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w_running = Window.partitionBy("job_title").orderBy(F.col("budget").asc_nulls_first())
w5 = Window.partitionBy(F.year("hire_date"), "dept_id").orderBy("rn", "id")
w6 = Window.partitionBy("dept_name", "job_title").orderBy("salary_cume_dist", "id")
w7 = Window.partitionBy("job_title", "dept_name")
w8 = Window.partitionBy("job_title").orderBy(F.col("budget_percent_rank").desc())
w9 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-08-25"))) & (F.col("level") != "L3"))
    .withColumn("level_lower", F.lower("level"))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

joined = (
    ranked
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.dept_name").isin(["Legal", "Engineering"]))
    .select("s.*", F.col("d.budget"), F.col("d.dept_name"))
)

scored = (
    joined
    .select(
        "id",
        "job_title",
        "salary",
        "is_remote",
        "level_lower",
        "rn",
        "dept_name",
        F.stddev_samp("budget").over(w_grp).alias("manager_id_stddev_samp_budget"),
        F.cume_dist().over(w3).alias("salary_cume_dist"),
        F.percent_rank().over(w_running).alias("budget_percent_rank"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

result = (
    scored
    .select(
        "*",
        F.lag("salary_cume_dist", 1).over(w6).alias("prev_salary_cume_dist"),
        F.sum("manager_id_stddev_samp_budget").over(w7).alias("job_title_sum_manager_id_stddev_samp_budget"),
        F.dense_rank().over(w8).alias("budget_percent_rank_dense_rank"),
        F.sum("manager_id_stddev_samp_budget").over(w9).alias("running_manager_id_stddev_samp_budget"),
    )
)
