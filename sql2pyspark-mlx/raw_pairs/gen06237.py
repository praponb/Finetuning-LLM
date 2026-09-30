from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("level").orderBy(F.col("salary").desc())
w3 = Window.partitionBy(F.date_trunc("YEAR", "hire_date"), "manager_id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.max("salary").alias("max_employee_salary"))

filtered = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "*",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_filled"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    ranked
    .select("dept_id", "id")
    .exceptAll(
        df_employees
        .filter(F.col("salary") > 58000)
        .select("dept_id", "id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .filter(F.col("manager_id").isin([32, 34]))
    .withColumn("salary_rank", F.rank().over(w2))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w3)), 4))
)

result = (
    with_windows
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "job_title_filled",
        "latest_rank",
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("max_employee_salary").desc())
)
