from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("full_name_length", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("job_title")
w_grp = Window.partitionBy(F.date_trunc("week", "hire_date"), "job_title").orderBy("full_name_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("manager_id")
w5 = Window.partitionBy("level").orderBy("salary_avg_2_prior", "id")

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-06-15").cast("date"))
    .withColumn("full_name_length", F.length("full_name"))
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
    .withColumn("level_clean", F.coalesce("level", F.lit("N/A")))
)

metrics = (
    valid
    .filter(F.col("full_name").like("%Won%"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "full_name_length",
        F.avg("salary").over(w).alias("salary_avg_2_prior"),
        F.max("full_name_length").over(w2).alias("job_title_max_full_name_length"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.budget"), F.col("b.dept_name"))
)

scored = (
    with_dims
    .withColumn("last_known_salary_avg_2_prior", F.last("salary_avg_2_prior", ignorenulls=True).over(w_grp))
    .withColumn("manager_id_min_job_title_max_full_name_length", F.min("job_title_max_full_name_length").over(w_recent))
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["employee_level", "employee_level"], ["employee_level"]],
        F.col("p.level").alias("employee_level"),
        F.col("src.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.budget")).alias("min_budget"),
        F.sum(F.col("src.manager_id_min_job_title_max_full_name_length")).alias("total_manager_id_min_job_title_max_full_name_length"),
        F.grouping_id().alias("gid"),
    )
)
